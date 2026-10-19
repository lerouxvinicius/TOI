(() => {
  const dialog = document.getElementById('toiAiDialog');
  const open = document.getElementById('toiAiOpen');
  const close = document.getElementById('toiAiClose');
  const form = document.getElementById('toiAiForm');
  const input = document.getElementById('toiAiQuestion');
  const send = document.getElementById('toiAiSend');
  const messages = document.getElementById('toiAiMessages');
  const history = [];
  if (!dialog || !form) return;
  const add = (kind, message, source) => {
    const node = document.createElement('div');
    node.className = 'toi-ai-entry ' + kind;
    node.textContent = message;
    if (source) {
      const small = document.createElement('small');
      small.textContent = 'Fonte: ' + source;
      node.appendChild(small);
    }
    messages.appendChild(node);
    messages.scrollTop = messages.scrollHeight;
    return node;
  };
  open?.addEventListener('click', () => { dialog.showModal(); input.focus(); });
  close?.addEventListener('click', () => dialog.close());
  window.addEventListener('toi-session-expired', () => {
    if (dialog.open) dialog.close();
    history.length = 0;
    messages.querySelectorAll('.toi-ai-entry').forEach(node => node.remove());
  });
  form.addEventListener('submit', async event => {
    event.preventDefault();
    const question = input.value.trim();
    if (!question || send.disabled) return;
    add('user', question);
    input.value = '';
    send.disabled = true;
    const pending = add('assistant', 'Consultando os dados locais…');
    try {
      const response = await fetch('/api/ai/ask', {
        method: 'POST', credentials: 'same-origin', cache: 'no-store',
        headers: {'Content-Type': 'application/json', 'X-CSRF-Token': window.toiCsrf || ''},
        body: JSON.stringify({question, history: history.slice(-4)}),
        signal: AbortSignal.timeout(150000)
      });
      if (!(response.headers.get('content-type') || '').toLowerCase().includes('application/json')) {
        throw new Error(response.status === 502 || response.status === 504
          ? 'A consulta demorou além do limite do TOI. Tente um período menor.'
          : 'O TOI retornou uma página inesperada. Atualize a tela e entre novamente.');
      }
      let data;
      try {
        data = await response.json();
      } catch (_) {
        throw new Error('A resposta do TOI veio incompleta. Tente novamente.');
      }
      if (!response.ok) throw new Error(data.error || 'Consulta indisponível');
      pending.textContent = data.answer || 'Sem resposta';
      const source = data.evidence?.map(item => item.source).filter(Boolean).join('; ');
      if (source) {
        const details = document.createElement('details');
        details.className = 'toi-ai-evidence';
        const summary = document.createElement('summary');
        summary.textContent = 'Fonte e cobertura';
        details.appendChild(summary);
        const small = document.createElement('small');
        const scope = data.scope || {};
        const coverage = (data.coverage || []).map(item => `${item.samples} amostras válidas de ${item.first} a ${item.last}`).join('; ');
        small.textContent = `Fonte: ${source}. Período consultado: ${scope.start_display || 'início disponível'} a ${scope.end_display || 'agora'}.${coverage ? ' Cobertura: ' + coverage + '.' : ''}`;
        details.appendChild(small);
        pending.appendChild(details);
      }
      history.push({role: 'user', content: question}, {role: 'assistant', content: data.answer || ''});
      if (history.length > 8) history.splice(0, history.length - 8);
    } catch (error) {
      pending.textContent = error.name === 'TimeoutError' ? 'A consulta excedeu o tempo disponível.' : (error.message || 'Consulta indisponível');
    } finally {
      send.disabled = false;
      messages.scrollTop = messages.scrollHeight;
      input.focus();
    }
  });
})();
