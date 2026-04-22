// Theme toggle — shared across all pages.
// Blocking init (loaded in <head>) restores persisted theme before
// first paint to prevent FOUC. Toggle button wired on DOMContentLoaded.

(function () {
    var stored = localStorage.getItem("theme");
    if (stored) document.documentElement.setAttribute("data-theme", stored);

    function isDark() {
        var t = document.documentElement.getAttribute("data-theme");
        if (t === "dark") return true;
        if (t === "light") return false;
        return window.matchMedia("(prefers-color-scheme: dark)").matches;
    }

    function syncIcon() {
        var btn = document.getElementById("theme-toggle");
        if (btn) btn.textContent = isDark() ? "\u2600" : "\u263E";
    }

    document.addEventListener("DOMContentLoaded", function () {
        syncIcon();
        var btn = document.getElementById("theme-toggle");
        if (!btn) return;
        btn.addEventListener("click", function () {
            var next = isDark() ? "light" : "dark";
            document.documentElement.setAttribute("data-theme", next);
            localStorage.setItem("theme", next);
            syncIcon();
        });
    });
})();
