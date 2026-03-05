/**
 * admin_auth.js — 前端管理鉴权模块
 *
 * 提供:
 *   window.AdminAuth.getToken()       — 获取当前 token（从 localStorage）
 *   window.AdminAuth.setToken(token)  — 保存 token
 *   window.AdminAuth.clearToken()     — 清除 token
 *   window.AdminAuth.prompt()         — 弹出输入框让用户输入 token
 *   window.AdminAuth.headers()        — 返回 { Authorization: 'Bearer ...' }
 *   window.adminFetch(url, opts)      — 替代 fetch()，自动注入 Authorization header
 *   window.adminEventSource(url)      — 替代 new EventSource()，通过 ?token= 查询参数鉴权
 */
(function () {
    'use strict';

    const STORAGE_KEY = 'cs_admin_token';

    const AdminAuth = {
        getToken() {
            return localStorage.getItem(STORAGE_KEY) || '';
        },

        setToken(token) {
            localStorage.setItem(STORAGE_KEY, (token || '').trim());
        },

        clearToken() {
            localStorage.removeItem(STORAGE_KEY);
        },

        /** Prompt user for admin token. Returns the token string or empty. */
        prompt(message) {
            const msg = message || '请输入管理密码 (ADMIN_TOKEN)：';
            const token = window.prompt(msg, this.getToken());
            if (token !== null) {
                this.setToken(token);
            }
            return this.getToken();
        },

        /** Returns headers object with Authorization. */
        headers() {
            const token = this.getToken();
            if (!token) return {};
            return { 'Authorization': `Bearer ${token}` };
        },

        /** Check if we have a token stored. */
        hasToken() {
            return !!this.getToken();
        },
    };

    /**
     * Drop-in replacement for fetch() that auto-injects the admin Bearer token.
     * If the server responds with 403, prompts for the token and retries once.
     */
    async function adminFetch(url, opts = {}) {
        const headers = Object.assign({}, AdminAuth.headers(), opts.headers || {});
        const mergedOpts = Object.assign({}, opts, { headers });

        let resp = await fetch(url, mergedOpts);

        // If 403, prompt for token and retry once
        if (resp.status === 403) {
            const token = AdminAuth.prompt('管理密码无效或未设置，请重新输入 ADMIN_TOKEN：');
            if (token) {
                const retryHeaders = Object.assign({}, { 'Authorization': `Bearer ${token}` }, opts.headers || {});
                const retryOpts = Object.assign({}, opts, { headers: retryHeaders });
                resp = await fetch(url, retryOpts);
            }
        }

        return resp;
    }

    /**
     * Create an authenticated EventSource using the native browser API.
     * Since native EventSource doesn't support custom headers, the token
     * is passed as a ?token= query parameter instead.
     * The backend require_admin() accepts both header and query param.
     *
     * Usage:
     *   const es = adminEventSource('/api/ai-analyze/stream?model=...');
     *   es.onopen = () => { ... };
     *   es.onmessage = (event) => { ... };
     *   es.onerror = () => { ... };
     */
    function adminEventSource(url) {
        let token = AdminAuth.getToken();
        if (!token) {
            token = AdminAuth.prompt('请输入管理密码 (ADMIN_TOKEN)：');
        }
        // Append token as query parameter
        const separator = url.includes('?') ? '&' : '?';
        const authedUrl = token ? `${url}${separator}token=${encodeURIComponent(token)}` : url;
        return new EventSource(authedUrl);
    }

    // Expose globally
    window.AdminAuth = AdminAuth;
    window.adminFetch = adminFetch;
    window.adminEventSource = adminEventSource;
})();
