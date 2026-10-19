/* Drag all TOI dialogs by their title/header without interfering with controls. */
(() => {
  let drag = null;
  const originalStyles = new WeakMap();
  const modalFor = target => target.closest('dialog, [role="dialog"]');
  const isHandle = (target, modal) => {
    const handle = target.closest('header, .ipbx-dialog-head, h2');
    return handle && modal.contains(handle) &&
      !target.closest('button, input, select, textarea, a, [role="button"]');
  };
  const clamp = (value, limit) => Math.min(Math.max(0, value), Math.max(0, limit));
  document.addEventListener('pointerdown', event => {
    if (event.button !== 0) return;
    const modal = modalFor(event.target);
    if (!modal || !isHandle(event.target, modal) ||
        (modal.tagName === 'DIALOG' && !modal.open)) return;
    const rect = modal.getBoundingClientRect();
    if (!originalStyles.has(modal)) originalStyles.set(modal, modal.getAttribute('style'));
    modal.style.setProperty('position', 'fixed', 'important');
    modal.style.setProperty('inset', 'auto', 'important');
    modal.style.setProperty('margin', '0', 'important');
    modal.style.setProperty('transform', 'none', 'important');
    modal.style.setProperty('left', `${rect.left}px`, 'important');
    modal.style.setProperty('top', `${rect.top}px`, 'important');
    drag = {modal, x: event.clientX, y: event.clientY, left: rect.left, top: rect.top};
    event.preventDefault();
  });
  document.addEventListener('pointermove', event => {
    if (!drag) return;
    const {modal, x, y, left, top} = drag;
    const rect = modal.getBoundingClientRect();
    modal.style.setProperty('left',
      `${clamp(left + event.clientX - x, window.innerWidth - rect.width)}px`, 'important');
    modal.style.setProperty('top',
      `${clamp(top + event.clientY - y, window.innerHeight - rect.height)}px`, 'important');
  });
  const finish = () => { drag = null; };
  const reset = modal => {
    if (!originalStyles.has(modal)) return;
    const original = originalStyles.get(modal);
    if (original === null) modal.removeAttribute('style');
    else modal.setAttribute('style', original);
    originalStyles.delete(modal);
  };
  document.addEventListener('pointerup', finish);
  document.addEventListener('pointercancel', finish);
  window.addEventListener('blur', finish);
  document.addEventListener('close', event => {
    if (event.target.tagName !== 'DIALOG') return;
    if (drag?.modal === event.target) finish();
    reset(event.target);
  }, true);
  document.querySelectorAll('.analytics-modal, .infra-inventory-modal').forEach(root => {
    new MutationObserver(() => {
      if (!root.classList.contains('open')) {
        root.querySelectorAll('[role="dialog"]').forEach(reset);
      }
    }).observe(root, {attributes:true, attributeFilter:['class']});
  });
})();
