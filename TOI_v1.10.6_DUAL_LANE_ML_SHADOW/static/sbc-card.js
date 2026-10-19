/* Card SBC existente: snapshot do /sbc e eventos confirmados; sem SNMP no navegador. */
(() => {
  const el = document.getElementById('sbcMonitorBody');
  if (!el) return;
  const dialog = document.getElementById('sbcTrafficDialog');
  const trafficBody = document.getElementById('sbcTrafficBody');
  const dateInput = document.getElementById('sbcTrafficDate');
  const hourInput = document.getElementById('sbcTrafficHour');
  const realmInput = document.getElementById('sbcTrafficRealm');
  const trafficStatus = document.getElementById('sbcTrafficStatus');
  const pairRealms = {
    norte:['a-alfa-2','a-internet-corpx','a-thetalink-corpx','a-alfa-4','a-alfa-4-stir'],
    sul:['a-beta-link1','a-beta-link2','a-Epsilon','a-Prefeitura']
  };
  let selectedPair = null;
  let trafficRequest = 0;
  const h = value => String(value ?? '—').replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num = value => value === null || value === undefined ? '—'
    : Number(value).toLocaleString('pt-BR', {maximumFractionDigits: 1});
  const pct = value => value === null || value === undefined ? '—'
    : `${Number(value).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}%`;
  const when = value => {
    const date = Date.parse(value || '');
    return Number.isFinite(date) ? new Date(date).toLocaleString('pt-BR') : 'não registrado';
  };
  const label = {
    NORMAL_OBSERVADO:'ACTIVE + STANDBY',
    DEGRADADO_OBSERVADO:'Redundância degradada',
    SEM_DADOS_RECENTES:'Sem dados recentes', DESCONHECIDO:'Estado HA não verificado'
  };
  const tone = state => state === 'NORMAL_OBSERVADO' ? 'normal'
    : state === 'DEGRADADO_OBSERVADO' ? 'warning' : 'unknown';
  function render(data) {
    const pairs = (data?.pairs || []).map(pair => {
      const nodes = (pair.nodes || []).map(node => `<div class="sbc-node">
        <div><b>${h(node.hostname || node.ip)}</b><small>${h(node.ip)} · ${h(node.role || 'Papel não observado')} · SNMP ${h(node.snmp)}</small></div>
        <div class="sbc-node-metrics">Health ${pct(node.health_pct)} · CPU ${pct(node.cpu_pct)} · Memória ${pct(node.memory_pct)}<br>SIP Sessions ${num(node.sip_sessions)} · CPS ${num(node.cps)}</div>
      </div>`).join('');
      const sw = pair.last_switchover?.detectado_em;
      const key = pair.name === 'SBCs NORTE' ? 'norte' : pair.name === 'SBCs SUL' ? 'sul' : '';
      return `<section class="sbc-pair" role="button" tabindex="0" data-sbc-pair="${key}" aria-label="Ver sessões e histórico de ${h(pair.name)}"><strong><span>${h(pair.name)} ↗</span><span class="sbc-state ${tone(pair.observed_ha)}">${h(label[pair.observed_ha] || pair.observed_ha)}</span></strong>
        ${nodes}
        <small class="sbc-meta ${pair.fresh ? '' : 'sbc-stale'}">SBC ${pair.fresh ? `lido ${h(when(pair.snapshot_at))} · ${num(pair.age_seconds)} s atrás` : `sem leitura recente · última ${h(when(pair.snapshot_at))}`} · coleta própria a cada 15 s</small>
        ${sw ? `<small class="sbc-meta">Último switchover: ${h(when(sw))}</small>` : ''}
      </section>`;
    }).join('');
    const today = new Date().toLocaleDateString('en-CA',{timeZone:'America/Sao_Paulo'});
    const events = (data?.sbc_incidentes || []).filter(event => {
      const stamp = Date.parse(event.confirmed_at || '');
      return Number.isFinite(stamp) && new Date(stamp).toLocaleDateString('en-CA',{timeZone:'America/Sao_Paulo'}) === today;
    }).slice(0, 5).map(event =>
      `<div class="sbc-event" data-event-id="${h(event.id)}"><span>${h(when(event.confirmed_at))}</span><span>${h(event.type)} · ${h(event.status)} · ${h(event.payload?.peer || event.payload?.agent || event.payload?.par || event.payload?.ip || '')}</span></div>`
    ).join('');
    const pairClocks = (data?.pairs || []).map(pair =>
      `${h(pair.name)}: ${pair.fresh ? `${num(pair.age_seconds)} s` : 'sem leitura recente'}`).join(' · ');
    const allFresh = (data?.pairs || []).length === 2 && data.pairs.every(pair => pair.fresh);
    el.innerHTML = `<div class="sbc-pairs">${pairs || '<div class="sbc-pair">Sem coleta SBC disponível.</div>'}</div>
      <small class="sbc-meta ${allFresh ? '' : 'sbc-stale'}">${pairClocks || 'Sem coleta SBC disponível'} · coleta própria a cada 15 s</small>
      <div class="sbc-events"><b>Incidentes confirmados · 5 min · hoje</b>${events || '<div class="sbc-event">Nenhum incidente SBC confirmado hoje.</div>'}</div>`;
  }
  const clock = value => value ? h(value.slice(11, 19)) : '—';
  const peak = value => value?.peak === null || value?.peak === undefined ? '—' : num(value.peak);
  function agentSection(data) {
    if (!data.current_fresh) return '<p class="sbc-traffic-muted sbc-stale">Agent Individual: sem leitura SBC recente.</p>';
    return (data.nodes || []).map(node => `<div class="sbc-agent-group">
      <h4>${h(node.hostname || node.ip)} · ${h(node.role || 'papel desconhecido')} · ${h(node.ip)}</h4>
      <div class="sbc-agent-row"><b>Agent Individual</b><b>Inbound</b><b>Outbound</b></div>
      ${(node.agents || []).map(agent => `<div class="sbc-agent-row">
        <span>${h(agent.name)} <small>${h(agent.ip)} · ${agent.observed ? h(agent.status === '3' ? 'IN SERVICE' : agent.status === '1' ? 'OUT OF SERVICE' : agent.status || 'estado desconhecido') : 'não localizado'}</small></span>
        <span>${num(agent.inbound_active)}</span><span>${num(agent.outbound_active)}</span>
      </div>`).join('') || '<p>Sem agentes configurados.</p>'}
    </div>`).join('');
  }
  function liveMarkup(data) {
    const observed = data.current;
    const current = data.realms.map(name => {
      const item = observed?.realms?.[name];
      const active = Number(item?.inbound || 0) > 0 || Number(item?.outbound || 0) > 0;
      const state = active ? 'Com tráfego' : item?.inbound === 0 && item?.outbound === 0 ? 'Sem sessões ativas' : 'Leitura indisponível';
      return `<div class="sbc-traffic-realm ${active ? 'active' : ''}"><b>${h(name)}</b><small>${state}</small>
        <span>Inbound Sessions: <strong>${num(item?.inbound)}</strong></span>
        <span>Outbound Sessions: <strong>${num(item?.outbound)}</strong></span></div>`;
    }).join('');
    const age = data.current_age_s === null ? '—' : num(data.current_age_s);
    return `<p class="sbc-traffic-muted ${data.current_fresh ? '' : 'sbc-stale'}">${data.current_fresh ?
      `Leitura ${h(when(observed.timestamp))} no SBC ${h(observed.sbc_ip || '—')} · idade ${age} s` :
      `Sem leitura SBC recente · idade ${age} s`} · coleta SNMP independente a cada 15 s; tela a cada 5 s.</p>
      <div class="sbc-traffic-grid">${current}</div>
      <section class="sbc-traffic-section"><h3>Agent Individual · leitura atual</h3>${agentSection(data)}</section>`;
  }
  function renderLive(data) {
    const target = document.getElementById('sbcTrafficLive');
    if (target) target.innerHTML = liveMarkup(data);
  }
  function renderTraffic(data) {
    const scope = data.hour === null ? 'Dia inteiro' : `${String(data.hour).padStart(2,'0')}:00–${String(data.hour).padStart(2,'0')}:59`;
    const summary = data.consolidated;
    const selected = data.selected_realms || data.realms;
    const counts = data.new_sessions;
    const countLabel = {complete:'Completa', collecting:'Até a última leitura',
      partial:'Parcial · mínimo conhecido', unavailable:'Indisponível'};
    const newCount = (item, direction) => item?.[direction] === null || item?.[direction] === undefined
      ? '—' : `${item.status === 'partial' ? '≥ ' : ''}${num(item[direction])}`;
    const realms = selected.map(name => {
      const item = data.realm_summary[name];
      const sessions = counts?.realms?.[name];
      const boundary = (sessions?.unallocated_inbound || 0) + (sessions?.unallocated_outbound || 0);
      return `<tr><td>${h(name)}</td>
        <td>${newCount(sessions,'inbound')}</td><td>${newCount(sessions,'outbound')}</td>
        <td>${peak(item.inbound)}</td><td>${clock(item.inbound.at)}</td>
        <td>${peak(item.outbound)}</td><td>${clock(item.outbound.at)}</td>
        <td>${item.inbound.readings}/${summary.samples}</td>
        <td title="${h((sessions?.reasons || []).join('; '))}">${h(countLabel[sessions?.status] || 'Indisponível')}
          ${sessions?.as_of ? `<br><small>Até ${clock(sessions.as_of)}</small>` : ''}
          ${boundary ? `<br><small>No limite da hora: ${num(sessions.unallocated_inbound)} inbound / ${num(sessions.unallocated_outbound)} outbound sem atribuição</small>` : ''}
        </td></tr>`;
    }).join('');
    const readings = data.readings.flatMap(row => selected.map(name =>
      `<tr><td>${clock(row.timestamp)}</td><td>${h(name)}</td>
        <td>${num(row.realms[name]?.inbound)}</td><td>${num(row.realms[name]?.outbound)}</td>
        <td>${h(row.sbc_ip || '—')}</td></tr>`)).join('');
    trafficBody.innerHTML = `
      <section class="sbc-traffic-section"><h3>Consolidado · ${h(data.date)} · ${h(scope)}</h3>
        <div class="sbc-traffic-summary">
          <div><small>Amostras</small><strong>${num(summary.samples)}</strong><small>${clock(summary.first_at)} a ${clock(summary.last_at)}</small></div>
          <div><small>Realms com tráfego</small><strong>${num(summary.realms_with_traffic)} / ${num(summary.total_realms)}</strong></div>
          <div><small>Maior leitura inbound em um realm</small><strong>${num(summary.inbound.peak)}</strong><small>${h(summary.inbound.realm || '—')} · ${clock(summary.inbound.at)}</small></div>
          <div><small>Maior leitura outbound em um realm</small><strong>${num(summary.outbound.peak)}</strong><small>${h(summary.outbound.realm || '—')} · ${clock(summary.outbound.at)}</small></div>
        </div>
      </section>
      <section class="sbc-traffic-section"><h3>Sessões novas no período</h3>
        <p class="sbc-traffic-muted">${counts?.first_collection_at ?
          `Histórico de novas sessões iniciado em ${h(when(counts.first_collection_at))}. Última coleta ${h(when(counts.latest_at))}.` :
          'Contagem de novas sessões ainda sem leituras disponíveis.'}
          Sessões que já existiam no início não são adicionadas. “≥” indica o mínimo conhecido em um período incompleto; “—” indica ausência de contagem, não zero.</p>
        <p class="sbc-traffic-muted">A contagem segue os novos incrementos informados pelo SBC em cada realm e sentido. Movimentos entre leituras que atravessam o limite da hora ficam sinalizados, sem atribuição por suposição. As sessões simultâneas continuam sendo mostradas como picos.</p>
      </section>
      <section class="sbc-traffic-section"><h3>Por realm · ${h(data.realm || 'Todos')}</h3>
        <div class="sbc-traffic-table-wrap"><table class="sbc-traffic-table"><thead><tr><th>Realm</th><th>Inbound novas</th><th>Outbound novas</th><th>Inbound pico</th><th>Às</th><th>Outbound pico</th><th>Às</th><th>Leituras</th><th>Contagem de novas sessões</th></tr></thead><tbody>${realms || '<tr><td colspan="9">Sem amostras no período.</td></tr>'}</tbody></table></div></section>
      ${data.hour === null ? '<p class="sbc-traffic-muted">Selecione uma hora para ver cada amostra e seu horário exato.</p>' :
        `<section class="sbc-traffic-section"><h3>Amostras das ${String(data.hour).padStart(2,'0')}:00</h3>
          <p class="sbc-traffic-muted">Cada número abaixo é a quantidade de sessões simultaneamente ativas naquele instante; não representa novas chamadas recebidas desde a amostra anterior.</p>
          <div class="sbc-traffic-table-wrap"><table class="sbc-traffic-table"><thead><tr><th>Horário</th><th>Realm</th><th>Inbound ativas</th><th>Outbound ativas</th><th>SBC ativo</th></tr></thead><tbody>${readings || '<tr><td colspan="5">Sem amostras nessa hora.</td></tr>'}</tbody></table></div></section>`}
      <section class="sbc-traffic-section"><h3>Leitura atual · independente do período</h3><div id="sbcTrafficLive">${liveMarkup(data)}</div></section>
      <p class="sbc-traffic-muted">Os picos são de sessões simultâneas, não totais de chamadas únicas no período. Picos de realms diferentes não são somados, pois uma chamada pode aparecer em mais de um realm. ${h(data.note)} “—” indica leitura ausente, não zero sessão.</p>`;
  }
  async function loadTraffic() {
    if (!selectedPair || !dialog?.open) return;
    const request = ++trafficRequest;
    trafficStatus.textContent = 'Atualizando histórico…';
    const params = new URLSearchParams({pair:selectedPair, date:dateInput.value});
    if (hourInput.value !== '') params.set('hour', hourInput.value);
    if (realmInput.value !== '') params.set('realm', realmInput.value);
    try {
      const response = await fetch(`/api/sbc-traffic?${params}`, {cache:'no-store'});
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
      if (request !== trafficRequest || !dialog.open) return;
      document.getElementById('sbcTrafficTitle').textContent = `${data.name} · sessões e histórico`;
      renderTraffic(data);
      trafficStatus.textContent = '';
    } catch (error) {
      if (request === trafficRequest) trafficStatus.textContent = `Consulta indisponível: ${error.message}`;
    }
  }
  let liveBusy = false;
  async function refreshLive() {
    if (!selectedPair || !dialog?.open || document.hidden || liveBusy) return;
    liveBusy = true;
    const pair = selectedPair;
    try {
      const response = await fetch(`/api/sbc-live?pair=${encodeURIComponent(pair)}`, {cache:'no-store'});
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (pair === selectedPair) renderLive(data);
    } catch (error) {
      const target = document.getElementById('sbcTrafficLive');
      if (target) target.textContent = `Leitura SBC indisponível: ${error.message}`;
    } finally { liveBusy = false; }
  }
  if (dialog) {
    hourInput.insertAdjacentHTML('beforeend', Array.from({length:24}, (_, value) =>
      `<option value="${value}">${String(value).padStart(2,'0')}:00</option>`).join(''));
    dateInput.max = new Date().toLocaleDateString('en-CA', {timeZone:'America/Sao_Paulo'});
    el.addEventListener('click', event => {
      const button = event.target.closest('[data-sbc-pair]');
      if (!button || !button.dataset.sbcPair) return;
      selectedPair = button.dataset.sbcPair;
      dateInput.value = new Date().toLocaleDateString('en-CA', {timeZone:'America/Sao_Paulo'});
      hourInput.value = '';
      realmInput.innerHTML = '<option value="">Todos</option>' +
        (pairRealms[selectedPair] || []).map(name => `<option value="${h(name)}">${h(name)}</option>`).join('');
      trafficRequest++;
      trafficStatus.textContent = '';
      document.getElementById('sbcTrafficTitle').textContent =
        `${selectedPair === 'norte' ? 'SBCs NORTE' : 'SBCs SUL'} · sessões e histórico`;
      trafficBody.innerHTML = '<div id="sbcTrafficLive" class="sbc-traffic-muted">Leitura atual em atualização…</div>' +
        '<p class="sbc-traffic-muted">Escolha dia, hora e realm e clique em Buscar para consultar o histórico.</p>';
      dialog.showModal();
      refreshLive();
    });
    el.addEventListener('keydown', event => {
      if ((event.key === 'Enter' || event.key === ' ') && event.target.matches('[data-sbc-pair]')) {
        event.preventDefault();
        event.target.click();
      }
    });
    document.getElementById('sbcTrafficClose').addEventListener('click', () => dialog.close());
    dialog.addEventListener('close', () => { trafficRequest++; trafficStatus.textContent = ''; });
    document.getElementById('sbcTrafficSearch').addEventListener('click', loadTraffic);
  }
  let busy = false;
  async function refresh() {
    if (busy || document.hidden) return;
    busy = true;
    try {
      const response = await fetch('/api/sbc-monitor', {cache:'no-store'});
      if (!response.ok) throw new Error(response.status === 401 ? 'Autenticação necessária' : `HTTP ${response.status}`);
      const data = await response.json();
      window.toiSbcMonitorFeed = data;
      render(data);
      if (typeof window.refreshSbcRecentIncidents === 'function') window.refreshSbcRecentIncidents();
    } catch (error) {
      el.textContent = `Monitor SBC indisponível: ${error.message}`;
    } finally { busy = false; }
  }
  window.addEventListener('toi-authenticated', refresh);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
  setInterval(refresh, 5000);
  setInterval(refreshLive, 5000);
  refresh();
})();
