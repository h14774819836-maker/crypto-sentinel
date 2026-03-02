(function () {
  const STATUS_TOKENS = {
    generic: {
      success: { tone: "success", icon: "check-circle-2", label_zh: "成功", label_en: "Success" },
      warn: { tone: "warning", icon: "alert-triangle", label_zh: "警告", label_en: "Warning" },
      warning: { tone: "warning", icon: "alert-triangle", label_zh: "警告", label_en: "Warning" },
      error: { tone: "danger", icon: "x-circle", label_zh: "错误", label_en: "Error" },
      danger: { tone: "danger", icon: "x-circle", label_zh: "错误", label_en: "Error" },
      info: { tone: "info", icon: "info", label_zh: "信息", label_en: "Info" },
      neutral: { tone: "neutral", icon: "dot", label_zh: "中性", label_en: "Neutral" },
      loading: { tone: "info", icon: "loader-2", label_zh: "处理中", label_en: "Loading" },
    },
    severity: {
      critical: { tone: "danger", icon: "siren", label_zh: "严重", label_en: "Critical" },
      high: { tone: "danger", icon: "alert-octagon", label_zh: "高", label_en: "High" },
      medium: { tone: "warning", icon: "alert-triangle", label_zh: "中", label_en: "Medium" },
      low: { tone: "info", icon: "shield-alert", label_zh: "低", label_en: "Low" },
    },
    health: {
      online: { tone: "success", icon: "circle-check-big", label_zh: "在线", label_en: "Online" },
      offline: { tone: "danger", icon: "circle-off", label_zh: "离线", label_en: "Offline" },
      stale: { tone: "warning", icon: "clock-alert", label_zh: "陈旧", label_en: "Stale" },
    },
  };

  const LIST_DEFAULTS = { initial: 100, batch: 50 };

  function safeLang() {
    try {
      return (localStorage.getItem("sentinel_lang") || "zh").toLowerCase();
    } catch (_e) {
      return "zh";
    }
  }

  function esc(v) {
    return String(v ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function attr(v) {
    return esc(v).replace(/`/g, "&#96;");
  }

  function ensureLucide() {
    if (window.lucide && typeof window.lucide.createIcons === "function") {
      window.lucide.createIcons();
    }
  }

  function getToastRoot() {
    let root = document.getElementById("sentinel-toast");
    if (!root) {
      root = document.createElement("div");
      root.id = "sentinel-toast";
      root.className = "hidden fixed top-4 right-4 z-[200] max-w-md glass-panel border px-4 py-3 rounded-xl shadow-xl text-sm";
      root.setAttribute("role", "status");
      root.setAttribute("aria-live", "polite");
      document.body.appendChild(root);
    }
    return root;
  }

  function showToast(msg, type = "info", options = {}) {
    const root = getToastRoot();
    const ttl = Number(options.ttl_ms || 4200);
    root.className =
      "fixed top-4 right-4 z-[200] max-w-md glass-panel border px-4 py-3 rounded-xl shadow-xl text-sm " +
      (type === "error"
        ? "border-red-400/30 text-red-200"
        : type === "success"
          ? "border-emerald-400/30 text-emerald-200"
          : type === "warning"
            ? "border-amber-400/30 text-amber-200"
            : "border-white/10 text-foreground");
    root.textContent = msg;
    root.classList.remove("hidden");
    clearTimeout(root._t);
    root._t = setTimeout(() => root.classList.add("hidden"), ttl);
    return root;
  }

  function tokenForStatus(kind, value) {
    const k = String(kind || "generic").toLowerCase();
    const v = String(value || "neutral").toLowerCase();
    return (STATUS_TOKENS[k] && STATUS_TOKENS[k][v]) || (STATUS_TOKENS.generic && STATUS_TOKENS.generic[v]) || STATUS_TOKENS.generic.neutral;
  }

  function statusLabel(token, customLabel) {
    if (customLabel) return customLabel;
    const lang = safeLang();
    return lang === "en" ? token.label_en : token.label_zh;
  }

  function renderStatusBadge(kind, value, opts = {}) {
    const token = tokenForStatus(kind, value);
    const label = statusLabel(token, opts.label);
    const spin = String(value).toLowerCase() === "loading" || opts.spin;
    return (
      `<span class="status-badge status-badge-${attr(token.tone)} ${opts.className || ""}"` +
      ` data-status-kind="${attr(kind || "generic")}" data-status-value="${attr(value || "neutral")}">` +
      `<i data-lucide="${attr(token.icon)}" class="w-3 h-3 ${spin ? "animate-spin" : ""}"></i>` +
      `<span>${esc(label)}</span>` +
      `</span>`
    );
  }

  function renderStatusDot(kind, value, opts = {}) {
    const token = tokenForStatus(kind, value);
    const label = statusLabel(token, opts.label);
    return (
      `<span class="status-inline ${opts.className || ""}" data-status-kind="${attr(kind || "generic")}" data-status-value="${attr(value || "neutral")}">` +
      `<span class="status-dot ${attr(token.tone)} ${opts.pulse ? "active" : ""}" aria-hidden="true"></span>` +
      `<span>${esc(label)}</span>` +
      `</span>`
    );
  }

  function applyStatusTokens(root = document) {
    root.querySelectorAll("[data-status-kind][data-status-value]:not([data-status-applied])").forEach((el) => {
      const kind = el.getAttribute("data-status-kind");
      const value = el.getAttribute("data-status-value");
      const mode = el.getAttribute("data-status-render") || "badge";
      const label = el.getAttribute("data-status-label") || "";
      if (mode === "dot") {
        el.innerHTML = renderStatusDot(kind, value, { label, pulse: el.hasAttribute("data-status-pulse") });
      } else {
        el.innerHTML = renderStatusBadge(kind, value, { label });
      }
      el.setAttribute("data-status-applied", "1");
    });
    ensureLucide();
  }

  function setLoading(btn, on, options = {}) {
    if (!btn) return false;
    if (on) {
      if (btn.dataset.loading === "1") return false;
      btn.dataset.loading = "1";
      if (!btn.dataset.originalHtml) btn.dataset.originalHtml = btn.innerHTML;
      btn.disabled = true;
      const text = options.text || btn.getAttribute("data-loading-text") || "处理中...";
      btn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i>${esc(text)}`;
      ensureLucide();
      return true;
    }
    btn.disabled = false;
    btn.dataset.loading = "0";
    if (btn.dataset.originalHtml) {
      btn.innerHTML = btn.dataset.originalHtml;
    }
    ensureLucide();
    return true;
  }

  async function withButtonLoading(btn, asyncFn, options) {
    if (!setLoading(btn, true, options)) return undefined;
    try {
      return await asyncFn();
    } finally {
      setLoading(btn, false);
    }
  }

  function resolveElement(target) {
    if (!target) return null;
    if (typeof target === "string") return document.querySelector(target);
    return target;
  }

  function ensureErrorSlot(target, className) {
    const el = resolveElement(target);
    if (!el) return null;
    let slot = el.querySelector(`:scope > .${className}`);
    if (!slot) {
      slot = document.createElement("div");
      slot.className = className;
      slot.setAttribute("role", "alert");
      slot.setAttribute("aria-live", "polite");
      el.prepend(slot);
    }
    return slot;
  }

  function showInlineError(target, msg) {
    const el = resolveElement(target);
    if (!el) return null;
    let slot = el.nextElementSibling;
    if (!slot || !slot.classList.contains("inline-error")) {
      slot = document.createElement("div");
      slot.className = "inline-error";
      slot.setAttribute("role", "alert");
      slot.setAttribute("aria-live", "polite");
      el.insertAdjacentElement("afterend", slot);
    }
    slot.textContent = msg;
    slot.classList.remove("hidden");
    return slot;
  }

  function showPanelError(panelId, msg, options = {}) {
    const panel = typeof panelId === "string" ? document.getElementById(panelId) || document.querySelector(panelId) : panelId;
    if (!panel) return null;
    const slot = ensureErrorSlot(panel, "panel-error");
    if (!slot) return null;
    slot.innerHTML = "";
    const text = document.createElement("div");
    text.className = "panel-error__text";
    text.textContent = msg;
    slot.appendChild(text);
    if (typeof options.retryAction === "function") {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "btn btn-secondary btn-xs mt-2";
      btn.textContent = options.retryLabel || "重试";
      btn.addEventListener("click", options.retryAction);
      slot.appendChild(btn);
    }
    slot.classList.remove("hidden");
    return slot;
  }

  function showPageError(containerId, msg) {
    const el = typeof containerId === "string" ? document.getElementById(containerId) || document.querySelector(containerId) : containerId;
    if (!el) return null;
    const slot = ensureErrorSlot(el, "page-error");
    if (!slot) return null;
    slot.textContent = msg;
    slot.classList.remove("hidden");
    return slot;
  }

  function clearError(scope) {
    const root = resolveElement(scope) || document;
    root.querySelectorAll(".inline-error,.panel-error,.page-error").forEach((el) => {
      el.classList.add("hidden");
      if (el.classList.contains("inline-error")) el.textContent = "";
      else el.innerHTML = "";
    });
  }

  async function copyText(text, options = {}) {
    try {
      await navigator.clipboard.writeText(String(text ?? ""));
      showToast(options.successLabel || "已复制", "success");
      return true;
    } catch (err) {
      showToast(options.errorLabel || `复制失败: ${err.message || err}`, "error");
      return false;
    }
  }

  function bindCopyButtons(root = document) {
    root.querySelectorAll("[data-copy-text]:not([data-copy-bound])").forEach((btn) => {
      btn.setAttribute("data-copy-bound", "1");
      if (!btn.getAttribute("aria-label")) {
        btn.setAttribute("aria-label", btn.getAttribute("title") || "Copy");
      }
      btn.addEventListener("click", async (e) => {
        e.preventDefault();
        e.stopPropagation();
        const text = btn.getAttribute("data-copy-text") || "";
        const successLabel = btn.getAttribute("data-copy-success") || "已复制";
        const errorLabel = btn.getAttribute("data-copy-error") || "复制失败";
        const old = btn.innerHTML;
        await withButtonLoading(btn, async () => {
          const ok = await copyText(text, { successLabel, errorLabel });
          if (ok) {
            btn.innerHTML = `<i data-lucide="check" class="w-3.5 h-3.5"></i>`;
            ensureLucide();
            setTimeout(() => {
              if (btn.dataset.loading !== "1") {
                btn.innerHTML = old;
                ensureLucide();
              }
            }, 900);
          }
        }, { text: "..." });
      });
    });
  }

  function persistToggle(key, initialState) {
    const fallback = !!initialState;
    function read() {
      try {
        const raw = localStorage.getItem(key);
        if (raw == null) return fallback;
        return raw === "1";
      } catch (_e) {
        return fallback;
      }
    }
    function write(state) {
      try {
        localStorage.setItem(key, state ? "1" : "0");
      } catch (_e) {
        // ignore
      }
      return state;
    }
    return {
      key,
      get: read,
      set: write,
      toggle() {
        return write(!read());
      },
    };
  }

  function bindPersistToggles(root = document) {
    root.querySelectorAll("[data-persist-toggle-key]:not([data-persist-bound])").forEach((btn) => {
      btn.setAttribute("data-persist-bound", "1");
      const key = btn.getAttribute("data-persist-toggle-key");
      const targetSel = btn.getAttribute("data-persist-toggle-target");
      const collapsedClass = btn.getAttribute("data-persist-collapsed-class") || "hidden";
      const expandedLabel = btn.getAttribute("data-persist-label-expanded") || "收起";
      const collapsedLabel = btn.getAttribute("data-persist-label-collapsed") || "展开";
      const target = targetSel ? document.querySelector(targetSel) : null;
      if (!target) return;
      const store = persistToggle(key, btn.getAttribute("data-persist-default") !== "collapsed");
      const labelNode = btn.querySelector("[data-persist-label]") || btn;
      const apply = (expanded) => {
        target.classList.toggle(collapsedClass, !expanded);
        btn.setAttribute("aria-expanded", expanded ? "true" : "false");
        labelNode.textContent = expanded ? expandedLabel : collapsedLabel;
        if (btn.dataset.iconExpanded && btn.dataset.iconCollapsed) {
          const icon = btn.querySelector("i[data-lucide]");
          if (icon) icon.setAttribute("data-lucide", expanded ? btn.dataset.iconExpanded : btn.dataset.iconCollapsed);
          ensureLucide();
        }
      };
      apply(store.get());
      btn.addEventListener("click", () => apply(store.toggle()));
    });
  }

  function progressiveRenderList(opts) {
    const {
      items = [],
      container,
      renderItem,
      initial = LIST_DEFAULTS.initial,
      batch = LIST_DEFAULTS.batch,
      moreContainer,
      emptyHtml = "",
    } = opts || {};
    const root = resolveElement(container);
    if (!root || typeof renderItem !== "function") return { rendered: 0, total: items.length };
    let count = 0;
    let visible = Math.min(items.length, initial);
    const render = () => {
      if (!items.length) {
        root.innerHTML = emptyHtml;
      } else {
        root.innerHTML = items.slice(0, visible).map(renderItem).join("");
      }
      bindCopyButtons(root);
      ensureLucide();
      if (moreContainer) {
        const more = resolveElement(moreContainer);
        if (more) {
          if (visible >= items.length) {
            more.innerHTML = "";
            more.classList.add("hidden");
          } else {
            more.classList.remove("hidden");
            more.innerHTML =
              `<button type="button" class="btn btn-secondary btn-sm" data-progressive-more>` +
              `加载更多 (${Math.min(batch, items.length - visible)})</button>` +
              `<span class="text-xs text-muted-foreground ml-2">${visible}/${items.length}</span>`;
            const btn = more.querySelector("[data-progressive-more]");
            if (btn) {
              btn.addEventListener("click", () => {
                visible = Math.min(items.length, visible + batch);
                render();
              });
            }
          }
        }
      }
      count = visible;
    };
    render();
    return { rendered: count, total: items.length };
  }

  function bindMobileSidebar(root = document) {
    const sidebar = root.querySelector("[data-mobile-sidebar]");
    const overlay = root.querySelector("[data-mobile-sidebar-overlay]");
    const openBtn = root.querySelector("[data-mobile-sidebar-open]");
    const closeBtn = root.querySelector("[data-mobile-sidebar-close]");
    if (!sidebar || !overlay) return;
    const open = () => {
      sidebar.classList.remove("hidden");
      overlay.classList.remove("hidden");
      requestAnimationFrame(() => {
        sidebar.classList.add("is-open");
        overlay.classList.add("is-open");
      });
      document.body.classList.add("mobile-nav-open");
    };
    const close = () => {
      sidebar.classList.remove("is-open");
      overlay.classList.remove("is-open");
      document.body.classList.remove("mobile-nav-open");
      setTimeout(() => {
        sidebar.classList.add("hidden");
        overlay.classList.add("hidden");
      }, 180);
    };
    if (openBtn && !openBtn.dataset.boundOpen) {
      openBtn.dataset.boundOpen = "1";
      openBtn.addEventListener("click", open);
    }
    if (closeBtn && !closeBtn.dataset.boundClose) {
      closeBtn.dataset.boundClose = "1";
      closeBtn.addEventListener("click", close);
    }
    if (!overlay.dataset.boundOverlay) {
      overlay.dataset.boundOverlay = "1";
      overlay.addEventListener("click", close);
    }
    root.querySelectorAll("[data-mobile-sidebar] a[href]").forEach((link) => {
      if (!link.dataset.boundNavClose) {
        link.dataset.boundNavClose = "1";
        link.addEventListener("click", close);
      }
    });
  }

  function syncLangToggleLabels(root = document) {
    const lang = safeLang();
    root.querySelectorAll("[data-lang-toggle]").forEach((btn) => {
      btn.textContent = lang === "zh" ? "EN" : "?";
    });
  }

  function decorateTables(root = document) {
    root.querySelectorAll("th,td").forEach((cell) => {
      const text = (cell.textContent || "").trim();
      if (!text) return;
      if (cell.classList.contains("col-num") || cell.classList.contains("col-time") || cell.classList.contains("col-tag")) return;
      if (/^\d{2}:\d{2}(:\d{2})?$/.test(text) || /^\d{4}-\d{2}-\d{2}/.test(text)) {
        cell.classList.add("col-time");
      } else if (/^-?\$?\d[\d,]*(\.\d+)?%?$/.test(text)) {
        cell.classList.add("col-num");
      }
    });
  }

  function bootPage(root = document) {
    bindMobileSidebar(root);
    syncLangToggleLabels(root);
    bindCopyButtons(root);
    bindPersistToggles(root);
    applyStatusTokens(root);
    decorateTables(root);
    ensureLucide();
  }

  window.SentinelUI = {
    STATUS_TOKENS,
    renderStatusBadge,
    renderStatusDot,
    tokenForStatus,
    setLoading,
    withButtonLoading,
    showToast,
    showInlineError,
    showPanelError,
    showPageError,
    clearError,
    copyText,
    persistToggle,
    progressiveRenderList,
    applyStatusTokens,
    bindCopyButtons,
    bootPage,
    esc,
    attr,
  };
})();
