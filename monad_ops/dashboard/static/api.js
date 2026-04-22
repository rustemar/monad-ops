// Copy-to-clipboard buttons for curl examples on /api page (D2.6).
// Attaches a button to each <pre class="example-code"> that copies
// the code block's text content.

document.querySelectorAll(".example-code").forEach(pre => {
    const btn = document.createElement("button");
    btn.className = "copy-btn";
    btn.textContent = "copy";
    btn.addEventListener("click", () => {
        const text = pre.textContent;
        navigator.clipboard.writeText(text).then(() => {
            btn.textContent = "copied";
            btn.classList.add("copied");
            setTimeout(() => {
                btn.textContent = "copy";
                btn.classList.remove("copied");
            }, 1500);
        }).catch(() => {
            btn.textContent = "copy failed";
            setTimeout(() => { btn.textContent = "copy"; }, 1500);
        });
    });
    pre.parentElement.appendChild(btn);
});
