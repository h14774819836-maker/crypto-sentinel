(function () {
  const bootstrap = window.STRATEGY_BOOTSTRAP || {};
  const MAX_LINES = Number(bootstrap.maxLines || 20);
  const DENSIFIED_RANGE_SECONDS = 3 * 24 * 3600;
  const AUTO_REFRESH_SECONDS = 3;
  const AUTO_REFRESH_LATEST_LIMIT = 300;
  const OHLCV_MAX_LIMIT = 9000;
  const OHLCV_EXTEND_PAD = 0.6;
  const RANGE_DEBOUNCE_MS = 200;
  const WS_RECONNECT_BASE_MS = 1000;
  const WS_RECONNECT_MAX_MS = 15000;

  const state = {
    symbol: String(bootstrap.defaultSymbol || "BTCUSDT").toUpperCase(),
    baseTf: "1h",
    scoringMode: "REALISTIC",
    side: "",
    outcome: "",
    regime: "",
    manifestId: "",
    fromTs: Math.floor(Date.now() / 1000) - 3600 * 500,
    toTs: Math.floor(Date.now() / 1000),
    cursor: null,
    rawItems: [],
    detailCache: new Map(),
    selectedDecisionIds: [],
    ohlcv: [],
    densified: [],
    mode: "raw",
    isLoading: false,
  };

  let chart;
  let candleSeries;
  let densitySeries;
  let priceLines = [];
  let rangeDebounceTimer = null;
  let lastFetchedFrom = 0;
  let lastFetchedTo = 0;
  let loadedOhlcvFrom = 0;
  let loadedOhlcvTo = 0;
  let skipRangeEvent = false;
  const DETAIL_CACHE_MAX = 200;
  let autoRefreshTimer = null;
  let wsConn = null;
  let wsReconnectTimer = null;
  let wsBackoff = WS_RECONNECT_BASE_MS;
  let wsActive = false;

  const els = {
    symbol: document.getElementById("strategy-symbol"),
    baseTf: document.getElementById("strategy-base-tf"),
    scoringMode: document.getElementById("strategy-scoring-mode"),
    side: document.getElementById("strategy-side"),
    outcome: document.getElementById("strategy-outcome"),
    regime: document.getElementById("strategy-regime"),
    manifestId: document.getElementById("strategy-manifest-id"),
    refresh: document.getElementById("strategy-refresh"),
    chart: document.getElementById("strategy-chart"),
    viewMeta: document.getElementById("strategy-view-meta"),
    decisionsMeta: document.getElementById("strategy-decisions-meta"),
    decisionList: document.getElementById("strategy-decision-list"),
    decisionDetail: document.getElementById("strategy-decision-detail"),
    loadMore: document.getElementById("strategy-load-more"),
    scores: document.getElementById("strategy-scores"),
    featureStats: document.getElementById("strategy-feature-stats"),
    maxLines: document.getElementById("strategy-max-lines"),
  };

  function esc(value) {
    return window.SentinelUI ? window.SentinelUI.esc(value) : String(value ?? "");
  }

  function numberOrNull(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function parseControlState() {
    state.symbol = String(els.symbol?.value || state.symbol || "BTCUSDT").toUpperCase();
    state.baseTf = String(els.baseTf?.value || "1h");
    state.scoringMode = String(els.scoringMode?.value || "REALISTIC").toUpperCase();
    state.side = String(els.side?.value || "").toUpperCase();
    state.outcome = String(els.outcome?.value || "").toUpperCase();
    state.regime = String(els.regime?.value || "").toUpperCase();
    state.manifestId = String(els.manifestId?.value || "").trim();
  }

  function formatTs(ts) {
    if (!ts) return "-";
    // Force Beijing Time (UTC+8)
    const d = new Date(Number(ts) * 1000 + 8 * 3600 * 1000);
    if (!Number.isFinite(d.getTime())) return "-";
    const Y = d.getUTCFullYear();
    const M = String(d.getUTCMonth() + 1).padStart(2, "0");
    const D = String(d.getUTCDate()).padStart(2, "0");
    const h = String(d.getUTCHours()).padStart(2, "0");
    const m = String(d.getUTCMinutes()).padStart(2, "0");
    const s = String(d.getUTCSeconds()).padStart(2, "0");
    return `${Y}-${M}-${D} ${h}:${m}:${s}`;
  }

  function buildApiUrl(path, params) {
    const qp = new URLSearchParams();
    Object.entries(params || {}).forEach(([k, v]) => {
      if (v === undefined || v === null || v === "") return;
      qp.set(k, String(v));
    });
    const s = qp.toString();
    return s ? `${path}?${s}` : path;
  }

  async function fetchJson(path, params) {
    const url = buildApiUrl(path, params);
    const resp = await fetch(url, { headers: { Accept: "application/json" } });
    if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
    return resp.json();
  }

  function buildWsUrl(symbol, timeframe) {
    const base = String(bootstrap.binanceWsUrl || "wss://stream.binance.com:9443/stream").trim();
    const stream = `${String(symbol).toLowerCase()}@kline_${String(timeframe).toLowerCase()}`;
    if (!base) return null;
    const hasQuery = base.includes("?");
    if (hasQuery) {
      if (base.includes("streams=")) {
        return base.replace(/streams=[^&]*/i, `streams=${stream}`);
      }
      return `${base}&streams=${stream}`;
    }
    return `${base}?streams=${stream}`;
  }

  function resetWsBackoff() {
    wsBackoff = WS_RECONNECT_BASE_MS;
  }

  function stopKlineWs(keepActive) {
    if (!keepActive) wsActive = false;
    if (wsReconnectTimer) {
      clearTimeout(wsReconnectTimer);
      wsReconnectTimer = null;
    }
    if (wsConn) {
      try {
        wsConn.close();
      } catch (_e) {
      }
      wsConn = null;
    }
  }

  function scheduleWsReconnect() {
    if (!wsActive) return;
    if (wsReconnectTimer) return;
    startAutoRefresh();
    wsReconnectTimer = setTimeout(() => {
      wsReconnectTimer = null;
      startKlineWs();
    }, wsBackoff);
    wsBackoff = Math.min(wsBackoff * 2, WS_RECONNECT_MAX_MS);
  }

  function applyRealtimeKline(kline) {
    const t = Math.floor(Number(kline.t) / 1000);
    if (!Number.isFinite(t)) return;
    const next = {
      time: t,
      open: Number(kline.o),
      high: Number(kline.h),
      low: Number(kline.l),
      close: Number(kline.c),
    };
    if (!Number.isFinite(next.open)) return;
    if (!last || t > last.time) {
      state.ohlcv.push(next);
      state.ohlcv = trimOhlcv(state.ohlcv);
      skipRangeEvent = true;
      candleSeries.update(next);
      requestAnimationFrame(() => { skipRangeEvent = false; });
    } else if (t === last.time) {
      state.ohlcv[state.ohlcv.length - 1] = next;
      state.ohlcv = trimOhlcv(state.ohlcv);
      skipRangeEvent = true;
      candleSeries.update(next);
      requestAnimationFrame(() => { skipRangeEvent = false; });
    } else {
      state.ohlcv = trimOhlcv(mergeOhlcv(state.ohlcv, [next]));
      skipRangeEvent = true;
      candleSeries.setData(state.ohlcv);
      requestAnimationFrame(() => { skipRangeEvent = false; });
    }
    loadedOhlcvTo = Math.max(loadedOhlcvTo, t);
  }

  function startKlineWs() {
    const wsUrl = buildWsUrl(state.symbol, state.baseTf);
    if (!wsUrl || typeof WebSocket === "undefined") {
      startAutoRefresh();
      return;
    }
    stopKlineWs(true);
    stopAutoRefresh();
    wsActive = true;
    try {
      wsConn = new WebSocket(wsUrl);
    } catch (_e) {
      scheduleWsReconnect();
      return;
    }
    wsConn.onopen = () => {
      resetWsBackoff();
      stopAutoRefresh();
    };
    wsConn.onmessage = (evt) => {
      try {
        const payload = JSON.parse(evt.data);
        const data = payload.data || payload;
        if (!data || data.e !== "kline") return;
        const kline = data.k || {};
        if (!kline || String(kline.i || "").toLowerCase() !== String(state.baseTf).toLowerCase()) return;
        applyRealtimeKline(kline);
      } catch (_e) {
      }
    };
    wsConn.onerror = () => {
      scheduleWsReconnect();
    };
    wsConn.onclose = () => {
      scheduleWsReconnect();
    };
  }

  function setChartLoading(loading) {
    const overlay = document.getElementById("strategy-chart-loading");
    if (overlay) overlay.classList.toggle("hidden", !loading);
  }

  function ensureChart() {
    if (chart) return;
    chart = LightweightCharts.createChart(els.chart, {
      autoSize: true,
      layout: {
        background: { type: 'solid', color: "transparent" },
        textColor: "rgba(148,163,184,0.9)",
      },
      localization: {
        timeFormatter: (time) => {
          // Force Beijing Time (UTC+8)
          const d = new Date(Number(time) * 1000 + 8 * 3600 * 1000);
          if (!Number.isFinite(d.getTime())) return "-";
          const Y = d.getUTCFullYear();
          const M = String(d.getUTCMonth() + 1).padStart(2, "0");
          const D = String(d.getUTCDate()).padStart(2, "0");
          const h = String(d.getUTCHours()).padStart(2, "0");
          const m = String(d.getUTCMinutes()).padStart(2, "0");
          return `${Y}-${M}-${D} ${h}:${m}`;
        },
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.04)" },
        horzLines: { color: "rgba(255,255,255,0.04)" },
      },
      crosshair: {
        mode: 0,
        vertLine: { width: 1, color: "rgba(148,163,184,0.4)", style: 3 },
        horzLine: { width: 1, color: "rgba(148,163,184,0.4)", style: 3 },
      },
      rightPriceScale: { borderColor: "rgba(148,163,184,0.2)" },
      timeScale: {
        borderColor: "rgba(148,163,184,0.2)",
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter: (time, tickMarkType, locale) => {
          // Force Beijing Time (UTC+8)
          const d = new Date(Number(time) * 1000 + 8 * 3600 * 1000);
          const M = String(d.getUTCMonth() + 1).padStart(2, '0');
          const D = String(d.getUTCDate()).padStart(2, '0');
          const h = String(d.getUTCHours()).padStart(2, '0');
          const m = String(d.getUTCMinutes()).padStart(2, '0');
          // 0: Year, 1: Month, 2: DayOfMonth, 3: Time, 4: TimeWithSeconds
          if (tickMarkType === 0) return String(d.getUTCFullYear());
          if (tickMarkType === 1 || tickMarkType === 2) return `${M}-${D}`;
          return `${h}:${m}`;
        }
      },
    });
    candleSeries = chart.addCandlestickSeries({
      upColor: "#2ebd85",
      downColor: "#f6465d",
      borderVisible: false,
      wickUpColor: "#2ebd85",
      wickDownColor: "#f6465d",
    });
    densitySeries = chart.addHistogramSeries({
      color: "rgba(56,189,248,0.35)",
      priceFormat: { type: "volume" },
      priceScaleId: "",
      lastValueVisible: false,
      priceLineVisible: false,
    });
    densitySeries.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
      if (skipRangeEvent) return;
      clearTimeout(rangeDebounceTimer);
      rangeDebounceTimer = setTimeout(onViewportChange, RANGE_DEBOUNCE_MS);
    });

    chart.subscribeClick((param) => {
      if (!param || !param.time || !state.rawItems.length) return;
      const time = Number(param.time);
      if (!Number.isFinite(time)) return;
      const exact = state.rawItems.find((item) => Number(item.decision_ts) === time);
      if (exact) {
        selectDecision(exact.id);
        return;
      }
      let nearest = null;
      let minDiff = Number.POSITIVE_INFINITY;
      for (const item of state.rawItems) {
        const diff = Math.abs(Number(item.decision_ts) - time);
        if (diff < minDiff) {
          nearest = item;
          minDiff = diff;
        }
      }
      if (nearest && minDiff <= 1800) selectDecision(nearest.id);
    });
  }

  function updateViewMeta() {
    const modeText = state.mode === "densified" ? "聚合" : "原始";
    const rangeHours = Math.max(0, ((state.toTs - state.fromTs) / 3600)).toFixed(1);
    if (els.viewMeta) {
      els.viewMeta.textContent = `${state.symbol} | ${state.baseTf} | 模式=${modeText} | 窗口=${rangeHours}h`;
    }
  }

  function renderDensitySeries() {
    if (!densitySeries) return;
    const data = (state.densified || []).map((b) => ({
      time: Number(b.bucket_ts),
      value: Number(b.count || 0),
      color: "rgba(56,189,248,0.35)",
    }));
    densitySeries.setData(data);
  }

  function markerColorForItem(item) {
    const side = String(item.position_side || "").toUpperCase();
    return side === "LONG" ? "rgba(34,197,94,1)" : side === "SHORT" ? "rgba(239,68,68,1)" : "rgba(148,163,184,0.9)";
  }

  function renderDecisionMarkers() {
    if (!candleSeries) return;

    // Use a default interval of 60 seconds if not parsed correctly.
    // If timeframeSeconds doesn't exist yet in the calling context, default to 60.
    const intervalSecs = (typeof timeframeSeconds === 'function') ? (timeframeSeconds(state.baseTf) || 60) : 60;

    // Group decisions by quantized timestamp
    const groups = new Map();
    for (const item of (state.rawItems || []).slice(0, 500)) {
      let ts = Number(item.decision_ts);
      ts = Math.floor(ts / intervalSecs) * intervalSecs; // Quantize
      if (!groups.has(ts)) groups.set(ts, []);
      groups.get(ts).push(item);
    }

    const markers = [];

    // Process each timestamp group
    for (const [ts, items] of groups.entries()) {
      // Group by side (LONG, SHORT, HOLD/Other) within the same timestamp
      const sideGroups = { LONG: [], SHORT: [], HOLD: [] };
      for (const item of items) {
        const side = String(item.position_side || "").toUpperCase();
        if (side === "LONG") sideGroups.LONG.push(item);
        else if (side === "SHORT") sideGroups.SHORT.push(item);
        else sideGroups.HOLD.push(item);
      }

      // Render LONG markers
      if (sideGroups.LONG.length > 0) {
        markers.push({
          time: ts,
          position: "belowBar",
          color: "rgba(34,197,94,1)",
          shape: "arrowUp",
          text: sideGroups.LONG.length > 1 ? `多x${sideGroups.LONG.length}` : "多",
          size: 1.5,
        });
      }

      // Render SHORT markers
      if (sideGroups.SHORT.length > 0) {
        markers.push({
          time: ts,
          position: "aboveBar",
          color: "rgba(239,68,68,1)",
          shape: "arrowDown",
          text: sideGroups.SHORT.length > 1 ? `空x${sideGroups.SHORT.length}` : "空",
          size: 1.5,
        });
      }

      // Render HOLD markers
      if (sideGroups.HOLD.length > 0) {
        markers.push({
          time: ts,
          position: "belowBar",
          color: "rgba(148,163,184,0.7)", // Slightly more transparent slate
          shape: "circle",
          text: sideGroups.HOLD.length > 1 ? `平x${sideGroups.HOLD.length}` : "平",
          size: 0.8, // Slightly smaller than default 1.5
        });
      }
    }

    markers.sort((a, b) => a.time - b.time);
    candleSeries.setMarkers(markers);
  }

  function clearPriceLines() {
    if (!candleSeries) return;
    for (const line of priceLines) {
      try {
        candleSeries.removePriceLine(line);
      } catch (_e) {
        // ignore
      }
    }
    priceLines = [];
  }

  function redrawSelectedLines() {
    clearPriceLines();
    if (!candleSeries) return;
    // Limit to only 1 max selected feature block to reduce chart clutter
    const maxDecisions = Math.max(1, Math.floor((MAX_LINES || 4) / 4));
    state.selectedDecisionIds = state.selectedDecisionIds.slice(-maxDecisions);

    const defs = [];
    for (const id of state.selectedDecisionIds) {
      const d = state.detailCache.get(id);
      if (!d) continue;
      if (d.entry_price) defs.push({ price: d.entry_price, color: "rgba(56,189,248,0.8)", title: `入场#${id}` });
      if (d.take_profit) defs.push({ price: d.take_profit, color: "rgba(34,197,94,0.8)", title: `止盈#${id}` });
      if (d.stop_loss) defs.push({ price: d.stop_loss, color: "rgba(239,68,68,0.8)", title: `止损#${id}` });
      if (d.liq_price_est) defs.push({ price: d.liq_price_est, color: "rgba(249,115,22,0.8)", title: `强平#${id}` });
    }

    const limits = MAX_LINES || 20;
    for (const def of defs.slice(0, limits)) {
      if (!Number.isFinite(def.price)) continue;
      const line = candleSeries.createPriceLine({
        price: Number(def.price),
        color: def.color,
        lineWidth: 1,
        lineStyle: 1,
        axisLabelVisible: true,
        title: def.title,
      });
      priceLines.push(line);
    }
  }

  function renderDecisionList(hasMore) {
    if (!els.decisionList) return;
    if (!state.rawItems.length) {
      els.decisionList.innerHTML = `<div class="text-xs text-slate-500 p-3 italic text-center">当前视图无决策记录。</div>`;
      if (els.loadMore) els.loadMore.classList.add("hidden");
      return;
    }
    const rows = state.rawItems
      .map((item) => {
        const outcome = String(item?.eval?.outcome_raw || item?.execution?.status || "OPEN").toUpperCase();
        const cls = state.selectedDecisionIds.includes(item.id) ? "bg-white/10 border-primary/40" : "bg-black/20 border-white/10";
        return (
          `<button type="button" class="w-full text-left border rounded px-2 py-1.5 hover:bg-white/10 ${cls}" data-decision-id="${item.id}">` +
          `<div class="flex justify-between items-center gap-2">` +
          `<span class="text-[11px] text-muted-foreground">${esc(formatTs(item.decision_ts))}</span>` +
          `<span class="text-[11px]">${esc(item.position_side || "-")} / ${esc(outcome)}</span>` +
          `</div>` +
          `<div class="text-xs mono mt-1">E:${esc(item.entry_price ?? "-")} TP:${esc(item.take_profit ?? "-")} SL:${esc(item.stop_loss ?? "-")}</div>` +
          `</button>`
        );
      })
      .join("");
    els.decisionList.innerHTML = rows;
    if (els.decisionsMeta) {
      els.decisionsMeta.textContent = `${state.rawItems.length} 条记录`;
    }
    els.decisionList.querySelectorAll("[data-decision-id]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const id = Number(btn.getAttribute("data-decision-id"));
        if (Number.isFinite(id)) selectDecision(id);
      });
    });
    if (els.loadMore) {
      els.loadMore.classList.toggle("hidden", !hasMore || state.mode !== "raw");
    }
  }

  function renderDetail(item) {
    if (!els.decisionDetail) return;
    els.decisionDetail.textContent = item ? JSON.stringify(item, null, 2) : "";
  }

  async function loadScores() {
    if (!els.scores || !els.featureStats) return;
    try {
      const common = {
        manifest_id: state.manifestId || undefined,
        scoring_mode: state.scoringMode,
      };
      const [scoresResp, statsResp] = await Promise.all([
        fetchJson("/api/strategy/scores", { ...common, limit: 8 }),
        fetchJson("/api/strategy/feature-stats", {
          ...common,
          regime_id: state.regime || undefined,
          status: "OK",
          limit: 20,
        }),
      ]);
      const scoreItems = scoresResp.items || [];
      const statItems = statsResp.items || [];

      els.scores.innerHTML = scoreItems.length
        ? scoreItems
          .map((s) => {
            const ci = `${Number(s.win_rate_ci_low ?? 0).toFixed(3)} ~ ${Number(s.win_rate_ci_high ?? 0).toFixed(3)}`;
            const badge = s.status === "INSUFFICIENT_DATA" ? "text-amber-300" : "text-emerald-300";
            return (
              `<div class="border border-white/10 rounded px-2 py-1 bg-black/20">` +
              `<div class="flex justify-between"><span>${esc(s.split_type)} / ${esc(s.scoring_mode)}</span><span class="${badge}">${esc(s.status)}</span></div>` +
              `<div class="text-muted-foreground">n=${esc(s.n_trades)} 胜率=${esc(s.win_rate ?? "-")} 置信区间=[${esc(ci)}]</div>` +
              `</div>`
            );
          })
          .join("")
        : `<div class="text-slate-500 text-center italic py-2">暂无评分数据。</div>`;

      els.featureStats.innerHTML = statItems.length
        ? statItems
          .map(
            (r) =>
              `<div class="border border-white/10 rounded px-2 py-1 bg-black/20">` +
              `<div>${esc(r.feature_key)} / ${esc(r.bucket_key)}</div>` +
              `<div class="text-muted-foreground">n=${esc(r.n)} win=${esc(r.win_rate ?? "-")} ci=[${esc(r.ci_low ?? "-")}, ${esc(r.ci_high ?? "-")}]</div>` +
              `</div>`,
          )
          .join("")
        : `<div class="text-slate-500 text-center italic py-2">暂无特征统计数据。</div>`;
    } catch (err) {
      els.scores.textContent = `scores load failed: ${err.message || err}`;
      els.featureStats.textContent = "";
    }
  }

  function parseOhlcvItems(respItems) {
    return (respItems || [])
      .map((row) => ({
        time: Number(row.ts),
        open: Number(row.open),
        high: Number(row.high),
        low: Number(row.low),
        close: Number(row.close),
      }))
      .filter((row) => Number.isFinite(row.time));
  }

  function mergeOhlcv(existing, incoming) {
    const map = new Map();
    for (const c of existing) map.set(c.time, c);
    for (const c of incoming) map.set(c.time, c);
    const merged = Array.from(map.values()).sort((a, b) => a.time - b.time);
    return merged;
  }

  function timeframeSeconds(value) {
    const raw = String(value || "").trim().toLowerCase();
    const match = raw.match(/^(\d+)([mhdw])$/);
    if (!match) return null;
    const n = Number(match[1]);
    if (!Number.isFinite(n) || n <= 0) return null;
    const unit = match[2];
    if (unit === "m") return n * 60;
    if (unit === "h") return n * 3600;
    if (unit === "d") return n * 86400;
    if (unit === "w") return n * 604800;
    return null;
  }

  function trimOhlcv(items) {
    const maxKeep = Math.max(OHLCV_MAX_LIMIT, AUTO_REFRESH_LATEST_LIMIT * 2);
    if (items.length <= maxKeep) return items;
    return items.slice(-maxKeep);
  }

  async function loadOhlcv(forceFullReplace) {
    const fetchFrom = state.fromTs;
    const fetchTo = state.toTs;
    const resp = await fetchJson("/api/ohlcv", {
      symbol: state.symbol,
      timeframe: state.baseTf,
      from: fetchFrom,
      to: fetchTo,
      limit: OHLCV_MAX_LIMIT,
    });
    const incoming = parseOhlcvItems(resp.items);
    if (forceFullReplace || !state.ohlcv.length) {
      state.ohlcv = incoming;
    } else {
      state.ohlcv = trimOhlcv(mergeOhlcv(state.ohlcv, incoming));
    }
    loadedOhlcvFrom = Math.min(loadedOhlcvFrom || fetchFrom, fetchFrom);
    loadedOhlcvTo = Math.max(loadedOhlcvTo || fetchTo, fetchTo);
    skipRangeEvent = true;
    candleSeries.setData(state.ohlcv);
    requestAnimationFrame(() => { skipRangeEvent = false; });
  }

  async function loadLatestOhlcvIncremental() {
    if (!state.ohlcv.length) {
      await loadViewportData(true);
      return;
    }
    const last = state.ohlcv[state.ohlcv.length - 1];
    const tfSeconds = timeframeSeconds(state.baseTf);
    const nowTs = Math.floor(Date.now() / 1000);
    const params = {
      symbol: state.symbol,
      timeframe: state.baseTf,
      limit: AUTO_REFRESH_LATEST_LIMIT,
    };
    if (last && tfSeconds) {
      params.from = Math.max(0, Number(last.time) - tfSeconds * 2);
      params.to = nowTs + tfSeconds;
    }
    const resp = await fetchJson("/api/ohlcv", params);
    const incoming = parseOhlcvItems(resp.items);
    if (!incoming.length) return;
    state.ohlcv = trimOhlcv(mergeOhlcv(state.ohlcv, incoming));
    const first = incoming[0];
    const lastIncoming = incoming[incoming.length - 1];
    if (first && lastIncoming) {
      loadedOhlcvFrom = loadedOhlcvFrom ? Math.min(loadedOhlcvFrom, first.time) : first.time;
      loadedOhlcvTo = Math.max(loadedOhlcvTo, lastIncoming.time);
    }
    skipRangeEvent = true;
    candleSeries.setData(state.ohlcv);
    requestAnimationFrame(() => { skipRangeEvent = false; });
  }

  async function loadRawDecisions(append) {
    const params = {
      symbol: state.symbol,
      from: state.fromTs,
      to: state.toTs,
      manifest_id: state.manifestId || undefined,
      side: state.side || undefined,
      outcome: state.outcome || undefined,
      regime: state.regime || undefined,
      mode: "raw",
      limit: 200,
      cursor: append ? state.cursor : undefined,
    };
    const resp = await fetchJson("/api/strategy/decisions", params);
    const items = Array.isArray(resp.items) ? resp.items : [];
    state.rawItems = append ? state.rawItems.concat(items) : items;
    state.cursor = resp.next_cursor || null;
    state.mode = "raw";
    renderDecisionMarkers();
    renderDecisionList(Boolean(resp.has_more));
  }

  async function loadDensified() {
    const range = state.toTs - state.fromTs;
    let bucketSeconds = 900;
    if (range > 14 * 24 * 3600) bucketSeconds = 3600;
    else if (range > 7 * 24 * 3600) bucketSeconds = 1800;
    const resp = await fetchJson("/api/strategy/decisions", {
      symbol: state.symbol,
      from: state.fromTs,
      to: state.toTs,
      manifest_id: state.manifestId || undefined,
      side: state.side || undefined,
      outcome: state.outcome || undefined,
      regime: state.regime || undefined,
      mode: "densified",
      bucket_seconds: bucketSeconds,
    });
    state.densified = resp.items || [];
    state.mode = "densified";
    renderDensitySeries();
  }

  async function loadViewportData(forceOhlcv) {
    if (state.isLoading) return;
    state.isLoading = true;
    setChartLoading(true);
    updateViewMeta();
    try {
      // Only fetch OHLCV if forced or viewport extends beyond loaded range
      const needOhlcv = forceOhlcv
        || loadedOhlcvTo === 0
        || state.fromTs < loadedOhlcvFrom
        || state.toTs > loadedOhlcvTo;
      if (needOhlcv) {
        // Extend the fetch range with padding to reduce future fetches
        const span = state.toTs - state.fromTs;
        const pad = Math.floor(span * OHLCV_EXTEND_PAD);
        const savedFrom = state.fromTs;
        const savedTo = state.toTs;
        state.fromTs = Math.min(state.fromTs, loadedOhlcvFrom || state.fromTs) - pad;
        state.toTs = Math.max(state.toTs, loadedOhlcvTo || state.toTs) + pad;
        try {
          await loadOhlcv(forceOhlcv);
        } catch (ohlcvErr) {
          if (window.SentinelUI) window.SentinelUI.showToast(`OHLCV load failed: ${ohlcvErr.message || ohlcvErr}`, "error");
        }
        // Restore viewport range for decisions (decisions filter by visible range)
        state.fromTs = savedFrom;
        state.toTs = savedTo;
      }
      const range = Math.max(0, state.toTs - state.fromTs);
      try {
        if (range >= DENSIFIED_RANGE_SECONDS) {
          await Promise.all([loadDensified(), loadRawDecisions(false)]);
        } else {
          skipRangeEvent = true;
          densitySeries.setData([]);
          requestAnimationFrame(() => { skipRangeEvent = false; });
          state.densified = [];
          await loadRawDecisions(false);
        }
      } catch (decErr) {
        if (window.SentinelUI) window.SentinelUI.showToast(`decisions load failed: ${decErr.message || decErr}`, "error");
      }
      lastFetchedFrom = state.fromTs;
      lastFetchedTo = state.toTs;
    } finally {
      state.isLoading = false;
      setChartLoading(false);
      updateViewMeta();
    }
  }

  async function selectDecision(decisionId) {
    if (!Number.isFinite(decisionId)) return;
    try {
      let detail = state.detailCache.get(decisionId);
      if (!detail) {
        const resp = await fetchJson(`/api/strategy/decisions/${decisionId}`);
        detail = resp.item;
        // Cap cache size
        if (state.detailCache.size >= DETAIL_CACHE_MAX) {
          const oldest = state.detailCache.keys().next().value;
          state.detailCache.delete(oldest);
        }
        state.detailCache.set(decisionId, detail);
      }
      if (!state.selectedDecisionIds.includes(decisionId)) {
        state.selectedDecisionIds.push(decisionId);
      }
      renderDetail(detail);
      redrawSelectedLines();
      renderDecisionList(Boolean(state.cursor));
    } catch (err) {
      if (window.SentinelUI) window.SentinelUI.showToast(`detail load failed: ${err.message || err}`, "error");
    }
  }

  function onViewportChange() {
    if (!chart || skipRangeEvent) return;
    const range = chart.timeScale().getVisibleRange();
    if (!range || range.from == null || range.to == null) return;
    const from = Math.floor(Number(range.from));
    const to = Math.ceil(Number(range.to));
    if (!Number.isFinite(from) || !Number.isFinite(to) || from >= to) return;
    // Skip if decision range barely changed (< 5% shift)
    const span = to - from;
    const shift = Math.abs(from - lastFetchedFrom) + Math.abs(to - lastFetchedTo);
    if (lastFetchedTo > 0 && shift < span * 0.05) return;
    state.fromTs = from;
    state.toTs = to;
    state.cursor = null;
    // loadViewportData will only fetch OHLCV if panning beyond loaded range
    // Zoom-in is safe — data already loaded
    loadViewportData(false);
  }

  function bindEvents() {
    [els.symbol, els.baseTf, els.scoringMode, els.side, els.outcome, els.regime].forEach((el) => {
      if (!el) return;
      el.addEventListener("change", async () => {
        parseControlState();
        state.cursor = null;
        state.selectedDecisionIds = [];
        state.detailCache.clear();
        clearPriceLines();
        // Reset OHLCV bounds on filter/symbol change
        loadedOhlcvFrom = 0;
        loadedOhlcvTo = 0;
        state.ohlcv = [];

        // Auto-scale to 500 candles
        const tfSecs = timeframeSeconds(state.baseTf) || 3600;
        const nowTs = Math.floor(Date.now() / 1000);
        state.toTs = nowTs;
        state.fromTs = nowTs - tfSecs * 500;

        await loadViewportData(true);
        await loadScores();
        if (chart) chart.timeScale().fitContent();
        startKlineWs();
      });
    });
    if (els.manifestId) {
      els.manifestId.addEventListener("change", async () => {
        parseControlState();
        state.cursor = null;
        await loadViewportData(true);
        await loadScores();
        startKlineWs();
      });
    }
    if (els.refresh) {
      els.refresh.addEventListener("click", async () => {
        parseControlState();
        state.cursor = null;
        // Force full reload on manual Refresh
        loadedOhlcvFrom = 0;
        loadedOhlcvTo = 0;
        state.ohlcv = [];
        await loadViewportData(true);
        await loadScores();
        startKlineWs();
      });
    }
    if (els.loadMore) {
      els.loadMore.addEventListener("click", async () => {
        if (!state.cursor || state.mode !== "raw") return;
        await loadRawDecisions(true);
      });
    }
  }

  function startAutoRefresh() {
    if (autoRefreshTimer) clearInterval(autoRefreshTimer);
    autoRefreshTimer = setInterval(() => {
      if (!chart || state.isLoading) return;
      loadLatestOhlcvIncremental().catch(() => { });
    }, AUTO_REFRESH_SECONDS * 1000);
  }

  function stopAutoRefresh() {
    if (autoRefreshTimer) {
      clearInterval(autoRefreshTimer);
      autoRefreshTimer = null;
    }
  }

  async function boot() {
    try {
      if (!els.chart) { els.viewMeta.textContent = 'NO CHART EL'; return; }
      if (typeof LightweightCharts === "undefined") { els.viewMeta.textContent = 'NO LWC LIB'; return; }
      if (els.maxLines) els.maxLines.textContent = String(MAX_LINES);
      parseControlState();
      ensureChart();
      bindEvents();
      await loadViewportData(true);
      await loadScores();
      if (chart) chart.timeScale().fitContent();
      startKlineWs();
    } catch (e) {
      if (els.viewMeta) els.viewMeta.textContent = 'CRASH: ' + String(e.message || e);
      console.error(e);
    }
  }

  document.addEventListener("DOMContentLoaded", boot);
})();
