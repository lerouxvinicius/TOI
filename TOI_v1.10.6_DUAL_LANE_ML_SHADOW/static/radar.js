/* R1.2.7 Radar UI R2 — queue identification in every alert surface. */
(() => {
  'use strict';
  const el=id=>document.getElementById(id);
  const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const when=value=>{
    if(!value)return 'Não registrado';
    const d=new Date(value);
    return Number.isFinite(+d)?new Intl.DateTimeFormat('pt-BR',{timeZone:'America/Sao_Paulo',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}).format(d):'Não registrado';
  };
  const resolvedWhen=value=>{
    const d=new Date(value);
    return Number.isFinite(+d)?new Intl.DateTimeFormat('pt-BR',{timeZone:'America/Sao_Paulo',day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}).format(d):'Não registrado';
  };
  const sentWhen=value=>{
    const d=new Date(value);if(!Number.isFinite(+d))return 'horário não registrado';
    const opts={timeZone:'America/Sao_Paulo'};
    const h=new Intl.DateTimeFormat('pt-BR',{...opts,hour:'2-digit',minute:'2-digit'}).format(d);
    const day=new Intl.DateTimeFormat('pt-BR',{...opts,day:'2-digit',month:'2-digit'}).format(d);
    return `às ${h} em ${day}`;
  };
  const pct=v=>v===null||v===undefined?'N/D':Number(v).toLocaleString('pt-BR',{maximumFractionDigits:2,minimumFractionDigits:2})+'%';
  const queueCaption=x=>{
    const code=String(x.codigo??'').trim();
    return code?` (${escape(code)})`:x.source==='PORTAL_ORBE'?' (código da fila não informado pela fonte)':'';
  };
  const labels={OPENED:'Novo incidente confirmado',PERSISTENT:'Congestionamento persistente',NORMALIZED:'Normalização confirmada',
    NORMALIZATION_OBSERVED:'Normalização observada no estado',IMPORTED_ACTIVE:'Incidente ativo importado',ACKNOWLEDGED:'Ciência registrada',SUPERSEDED:'Substituído por novo ciclo; encerramento anterior sem horário confirmado',AUTO_CLEANUP:'Limpeza automática de canais',OBSERVED:'Evento confirmado'};
  let data=null,mode='now',paused=matchMedia('(prefers-reduced-motion: reduce)').matches,hover=false;
  let period=null,facts=[],history=[],nextCursor=null,historyKey='',historyVersion=0,historyAt=0,historyBusy=false,historyError='';
  let selected=null,resolvedSelected=null,ackBusy=false,sound=null,soundEnabled=false,lastSoundId='',pollBusy=false,previousFocus=null;
  let scrollRest=0;
  const fetchJson=async(url,options={})=>{
    const response=await fetch(url,{cache:'no-store',signal:AbortSignal.timeout(6000),...options});
    const result=await response.json();
    if(!response.ok)throw new Error(result.error||`HTTP ${response.status}`);
    return result;
  };
  const title=x=>x.source==='AUTO_LIMPEZA'?'Limpeza automática de canais':x.source==='AGIROUTE'?(x.kind==='HOST_UNREACHABLE'?'Agiroute · acesso SSH indisponível':x.kind==='CHECK_FAILED'?'Agiroute · verificação indisponível':x.kind==='BOOT_DISABLED'?'Agiroute · início automático desabilitado':'Agiroute · serviço indisponível'):x.source==='MONITOR_ALERTAS'?(x.projeto==='SBC'?'SBC · '+x.entity_key:'MGW · '+x.entity_key):x.source==='GATEWAY'?(x.kind==='LOAD_IMBALANCE'?'Gateways · distribuição de tráfego desequilibrada':x.kind==='ICMP'?'Gateway sem resposta ICMP':x.status==='NORMALIZED'?'Gateway voltou a receber canais válidos ocupados':'Gateway sem ocupação observada'):'Portal Orbe · congestionamento';
  const balanceSummary=x=>`${x.investigation?.low_gateways?.length||0} gateways com baixa ocupação · pares ${pct(x.investigation?.peer_median_pct)} · ${x.investigation?.congested_queues?.length||0} fila(s) com P135 ≥ 20%. Causa em investigação.`;
  const lifecycle=x=>x.source==='AUTO_LIMPEZA'?`Resultado: ${x.status}`:x.status==='OBSERVED'?'Evento confirmado':!x.active?'Normalizado':x.stale?'Ativo no último estado · dados antigos':x.evidence_available===false?'Ativo · confirmação suspensa por falta de evidência':x.status==='NORMALIZATION_PENDING'?'Aguardando confirmação de normalização':'Incidente ativo';
  const delivery=x=>{
    const d=x.delivery||{},kind=d.kind||x.last_event_kind||x.kind;
    if(d.status==='SENT'&&d.sent_at){
      const what=kind==='NORMALIZED'?'Normalização':kind==='PERSISTENT'?'Alerta de persistência':'Alerta';
      return `<p class="radar-sent">${what} enviado ao Telegram ${escape(sentWhen(d.sent_at))}.</p>`;
    }
    const messages={DISABLED:'Telegram desabilitado neste envio.',ATTEMPTED:'Envio ao Telegram iniciado; confirmação pendente.',
      UNCONFIRMED:'Envio ao Telegram sem confirmação; reenvio automático suspenso para evitar duplicidade.',
      QUEUED:'Alerta aguardando envio ao Telegram.',RETRY:'Telegram indisponível; nova tentativa agendada.',
      FAILED:'Telegram recusou o envio. Verifique a configuração do bot e do grupo.',
      CANCELLED:'Envio cancelado: condição encerrada antes do envio.',
      NOT_SENT:'Alerta local do TOI.',NOT_RECORDED:'Envio ao Telegram sem comprovante no histórico disponível.'};
    return `<p class="radar-unknown">${escape(messages[d.status]||messages.NOT_RECORDED)}</p>`;
  };
  const actions=x=>`<div class="radar-item-actions"><button type="button" data-radar-open="${escape(x.id)}">${x.ack_at?'Ver registro / análise':'Registrar ciência'}</button></div>`;
  const item=x=>`<article class="radar-item ${x.active?'active':x.status==='SUPERSEDED'?'stale':'normal'}">
    <header><b>${escape(title(x))}</b><span>Inst. ${escape(x.instancia)}</span></header>
    <p class="radar-entity">${escape(x.entity)}${queueCaption(x)}</p>
    ${x.projeto?`<p>${escape(x.projeto)}</p>`:''}
    <p class="radar-values">${x.source==='PORTAL_ORBE'?`P34 ${pct(x.p34)} · P135 ${pct(x.p135)}`:x.source==='AUTO_LIMPEZA'?`${escape(x.channels_stuck??'—')} canais travados · ${escape(x.channels_released??'—')} liberados`:x.source==='MONITOR_ALERTAS'||x.source==='AGIROUTE'?`Estado confirmado: ${escape(x.condition)}`:escape(x.kind==='LOAD_IMBALANCE'?balanceSummary(x):x.kind==='ICMP'?'Sem resposta ICMP confirmada por 10 min':'Sem ocupação com expectativa de tráfego') }</p>
    <p>${escape(lifecycle(x))}</p><p class="radar-time">Início: ${escape(when(x.started_at))}</p>
    ${x.normalization_started_at&&x.active?`<p class="radar-time">Normalização em observação desde ${escape(when(x.normalization_started_at))}</p>`:''}
    ${x.traffic_returned_at?`<p class="radar-time">Canais válidos ocupados retornaram em ${escape(when(x.traffic_returned_at))}</p>`:''}
    ${x.source==='GATEWAY'&&x.kind==='NO_TRAFFIC'&&x.normalization_confirmed?`<p class="radar-time">Normalização confirmada em ${escape(when(x.resolved_at))}</p>`:''}
    ${x.stale?`<p class="radar-stale">Última amostra: ${escape(when(x.last_sample_at))}. A ausência de atualização não encerra o incidente.</p>`:''}
    ${delivery(x)}<p class="radar-time">${x.ack_at?`Ciente: ${escape(when(x.ack_at))}`:'Ciência pendente'}</p>${actions(x)}</article>`;
  const eventItem=x=>`<article class="radar-item radar-fact ${['NORMALIZED','NORMALIZATION_OBSERVED'].includes(x.kind)?'green':''}">
    <header><b>${escape(labels[x.kind]||x.kind)}${x.diagnostic_kind==='LOAD_IMBALANCE'?' · distribuição dos gateways':''}</b><span>Inst. ${escape(x.instancia)}</span></header>
    <p class="radar-entity">${escape(x.entity)}${queueCaption(x)}</p>
    ${x.kind==='AUTO_CLEANUP'?`<p>${escape(x.channels_stuck??'—')} canais travados · ${escape(x.channels_released??'—')} liberados</p>`:''}
    <p class="radar-time">${escape(when(x.occurred_at))}</p>${x.kind==='ACKNOWLEDGED'?'':delivery(x)}</article>`;
  function setHTML(node,html){
    if(!node)return;const offset=node.scrollTop;
    if(node.innerHTML!==html)node.innerHTML=html;
    node.scrollTop=offset;
  }
  function activeHtml(){
    if(!data)return '<div class="radar-empty">Carregando o Radar…</div>';
    const rows=[...(data.items||[])].sort((a,b)=>Number(!b.ack_at)-Number(!a.ack_at)||Number(b.active)-Number(a.active)||Date.parse(a.started_at)-Date.parse(b.started_at));
    const recent=(data.events||[]).filter(x=>['NORMALIZED','NORMALIZATION_OBSERVED'].includes(x.kind)).slice(0,10);
    const empty=Object.values(data.sources||{}).some(x=>!['OK','NOT_STARTED'].includes(x))?'Fontes indisponíveis ou em atualização. Não é possível afirmar ausência de incidentes.':'Nenhum incidente ativo ou aguardando ciência.';
    return rows.map(item).join('')+recent.map(eventItem).join('')||`<div class="radar-empty">${empty}</div>`;
  }
  function periodHtml(){
    if(!period)return '<div class="radar-empty">Carregando o período selecionado…</div>';
    const rows=facts.map((x,i)=>({id:'fact_'+i,ts:Date.parse(x.ts)||0,priority:x.priority||0,
      html:`<article class="radar-item radar-fact ${escape(x.color)}"><header><b>${escape(x.fullText||x.text)}</b></header><p>${escape(x.val)}</p><p class="radar-time">${x.ts?escape(when(x.ts)):'Resumo do período'} · fato observado</p></article>`}));
    const eventIds=new Set();
    for(const x of history)if(!eventIds.has(x.id)){eventIds.add(x.id);rows.push({id:x.id,ts:Date.parse(x.occurred_at)||0,priority:0,html:eventItem(x)});}
    rows.sort((a,b)=>b.ts-a.ts||b.priority-a.priority||a.id.localeCompare(b.id));
    const note=historyError?`<div class="radar-empty">${escape(historyError)}<button type="button" data-radar-history-retry>Recarregar</button></div>`:'';
    return note+(rows.map(x=>x.html).join('')||'<div class="radar-empty">Sem eventos registrados neste período. Histórico anterior à ativação do Radar não contém comprovantes de envio.</div>');
  }
  function render(){
    if(data){
      const c=data.counts||{};
      el('radarCounts').innerHTML=`<span>Portal Orbe: <b>${Number(c.portal_active)||0}</b> ativos</span><span> · Gateways: <b>${Number(c.gateway_active)||0}</b></span><span> · Agiroute: <b>${Number(c.agiroute_active)||0}</b></span><span> · MGW/SBC: <b>${Number(c.monitor_active)||0}</b></span><button type="button" data-radar-pending>${Number(c.ack_pending)||0} sem ciência</button>`;
      const bad=Object.entries(data.sources||{}).filter(([,v])=>!['OK','NOT_STARTED'].includes(v));
      const oldTick=!data.last_tick||Date.now()-Date.parse(data.last_tick)>20000;
      const text=bad.length?`Fonte em atenção: ${bad.map(([k])=>({portal:'Portal Orbe',gateway:'Gateways',agiroute:'Agiroute Instância 6',gateway_telegram:'Telegram de infraestrutura',journal:'histórico Telegram',store:'registro do Radar'}[k]||k)).join(', ')}`:oldTick?'Radar aguardando atualização das fontes':`Atualizado ${when(data.last_tick)}`;
      const telegramNote=data.gateway_telegram?.configured?' · Telegram de infraestrutura ativo':data.sources?.journal==='NOT_STARTED'?' · Telegram: aguardando primeiro registro':'';
      el('radarSource').textContent=text+telegramNote+(data.truncated?' · Lista limitada a 1.000 registros; consulte o histórico':'');
      el('radarSource').classList.toggle('warn',bad.length>0||oldTick);
    }
    setHTML(el('radarNow'),activeHtml());setHTML(el('timeline'),periodHtml());
    el('radarPeriodScope').textContent=period?`Instância ${period.instancia} · fila ${period.codigo} · ${period.label}. Fatos observados e eventos confirmados em ordem cronológica.`:'';
    el('radarMore').hidden=!nextCursor||mode!=='period';
    el('radarMore').disabled=historyBusy;
    if(el('radarExpanded').open)setHTML(el('radarExpandedBody'),mode==='now'?activeHtml():periodHtml()+(nextCursor?'<button type="button" data-radar-more>Carregar mais eventos</button>':''));
  }
  function showMode(value){
    mode=value;
    el('radarNow').hidden=value!=='now';el('radarPeriod').hidden=value!=='period';
    for(const [id,tab] of [['radarNowTab','now'],['radarPeriodTab','period']]){
      el(id).setAttribute('aria-selected',String(value===tab));el(id).tabIndex=value===tab?0:-1;
    }
    if(value==='period')loadHistory(false);render();
  }
  async function loadHistory(more=false){
    if(!period?.start||!period?.end||historyBusy||(more&&!nextCursor))return;
    const key=JSON.stringify(period),version=++historyVersion;
    if(!more&&key===historyKey&&Date.now()-historyAt<15000)return;
    historyBusy=true;historyError='';
    const query=new URLSearchParams({start:period.start,end:period.end,instancia:period.instancia,codigo:period.codigo});
    if(more)query.set('before',nextCursor);
    try{
      const r=await fetchJson('/api/radar/history?'+query);
      if(version!==historyVersion||key!==JSON.stringify(period))return;
      history=more?history.concat(r.events||[]):r.events||[];
      nextCursor=r.next_cursor;historyKey=key;historyAt=Date.now();
    }catch(e){if(version===historyVersion)historyError='Histórico de alertas indisponível. Os fatos do período continuam visíveis.';}
    finally{if(version===historyVersion){historyBusy=false;render();}}
  }
  function setPeriod(scope,rows){
    const changed=JSON.stringify(scope)!==JSON.stringify(period);
    period=scope;facts=rows||[];
    if(changed){historyVersion++;historyBusy=false;history=[];nextCursor=null;historyError='';historyKey='';}
    if(mode==='period')loadHistory(false);
    render();
  }
  function beep(id){
    if(!soundEnabled||!sound||id===lastSoundId||sound.state!=='running')return;
    lastSoundId=id;
    for(let i=0;i<2;i++){
      const oscillator=sound.createOscillator(),gain=sound.createGain(),t=sound.currentTime+i*.3;
      oscillator.frequency.value=740;gain.gain.setValueAtTime(.06,t);gain.gain.exponentialRampToValueAtTime(.001,t+.18);
      oscillator.connect(gain);gain.connect(sound.destination);oscillator.start(t);oscillator.stop(t+.2);
    }
  }
  function fillDialog(){
    if(!selected)return;
    el('radarAlertTitle').textContent=title(selected);
    el('radarAlertBody').innerHTML=`<p><b>Instância ${escape(selected.instancia)} · ${escape(selected.entity)}${queueCaption(selected)}</b></p><p>${escape(selected.projeto||'')}</p>
      <p>${escape(lifecycle(selected))}</p>${selected.source==='PORTAL_ORBE'?`<p>P34: ${pct(selected.p34)} · P135: ${pct(selected.p135)}</p>`:''}
      <dl><dt>Início da condição</dt><dd>${escape(when(selected.started_at))}</dd><dt>Confirmação do incidente</dt><dd>${escape(when(selected.confirmed_at))}</dd><dt>Detectado pelo Radar</dt><dd>${escape(when(selected.detected_at))}</dd><dt>Última amostra</dt><dd>${escape(when(selected.last_sample_at))}</dd><dt>Ciência</dt><dd>${escape(when(selected.ack_at))}</dd></dl>
      ${selected.kind==='LOAD_IMBALANCE'?`<p>${escape(balanceSummary(selected))}</p>${typeof window.toiGatewayInvestigationHtml==='function'?window.toiGatewayInvestigationHtml(selected.stale?{status:'DADOS_ATRASADOS',valid:false}:selected.investigation):''}`:''}
      ${selected.stale?'<p class="radar-stale">Dados antigos: a condição atual precisa de uma nova amostra.</p>':''}${delivery(selected)}
      <p>“Ciente” registra que este incidente foi visto. O envio ao Telegram não comprova leitura por uma pessoa.</p>`;
    el('radarAcknowledge').disabled=ackBusy||!!selected.ack_at;
    el('radarAcknowledge').textContent=selected.ack_at?'Ciência registrada':ackBusy?'Gravando…':'Ciente';
    el('radarAnalyze').disabled=!selected.ack_at||ackBusy;
    el('radarAlertNext').hidden=!selected.ack_at;
  }
  function openIncident(x){
    if(!x)return;
    if(el('radarExpanded').open)el('radarExpanded').close();
    selected=x;el('radarAckStatus').textContent='';fillDialog();
    if(!el('radarAlert').open){previousFocus=document.activeElement;el('radarAlert').showModal();}
    (selected.ack_at?el('radarAnalyze'):el('radarAcknowledge')).focus();
    if(!selected.ack_at)beep(selected.id);
  }
  function showPending(){
    if(el('radarAlert').open||el('radarResolved').open||el('radarExpanded').open)return;
    const recovered=(data?.items||[]).filter(x=>x.normalization_ack_pending&&!x.active&&x.source==='PORTAL_ORBE')
      .sort((a,b)=>Date.parse(a.resolved_at)-Date.parse(b.resolved_at));
    if(recovered.length){openResolution(recovered[0]);return;}
    const pending=(data?.items||[]).filter(x=>!x.ack_at).sort((a,b)=>Date.parse(a.started_at)-Date.parse(b.started_at));
    if(pending.length)openIncident(pending[0]);
  }
  function openResolution(x){
    resolvedSelected=x;
    el('radarResolvedEyebrow').textContent=`RADAR OPERACIONAL · INSTÂNCIA ${x.instancia}`;
    el('radarResolvedBody').innerHTML=`<strong class="resolved-queue">${escape(x.entity)}${x.codigo?' - '+escape(x.codigo):''}</strong>
      <time>${escape(resolvedWhen(x.resolved_at))}</time>
      <div class="resolved-metrics"><div><b>Na Companhia</b><strong>${x.p34===0?'0%':pct(x.p34)}</strong></div><div><b>Interno</b><strong>${x.p135===0?'0%':pct(x.p135)}</strong></div></div>
      <p class="resolved-rule">P34 e P135 ficaram abaixo de 20% durante ${escape(x.normalization_minutes||10)} minutos contínuos.</p>
      <p class="resolved-guidance">Registre sua ciência para continuar.</p>`;
    el('radarResolvedStatus').textContent='';
    previousFocus=document.activeElement;
    el('radarResolved').showModal();
    el('radarResolvedAcknowledge').focus();
    beep(x.id+':resolved');
  }
  async function acknowledgeResolution(){
    if(!resolvedSelected||ackBusy)return;
    ackBusy=true;const button=el('radarResolvedAcknowledge');button.disabled=true;
    el('radarResolvedStatus').textContent='Gravando ciência…';
    try{
      const r=await fetchJson('/api/radar/ack-normalization',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({incident_id:resolvedSelected.id})});
      if(!r.ok||!r.incident?.normalization_ack_at)throw new Error('Ciência não confirmada');
      const i=data?.items?.findIndex(x=>x.id===resolvedSelected.id);
      if(i>=0)data.items[i]=r.incident;
      if(data?.counts)data.counts.ack_pending=Math.max(0,data.counts.ack_pending-1);
      el('radarResolved').close();resolvedSelected=null;render();previousFocus?.focus?.();showPending();
    }catch(_error){el('radarResolvedStatus').textContent='Não foi possível registrar a ciência. Tente novamente.';}
    finally{ackBusy=false;button.disabled=false;}
  }
  async function acknowledge(){
    if(!selected||selected.ack_at||ackBusy)return;
    ackBusy=true;fillDialog();el('radarAckStatus').textContent='Gravando ciência…';
    try{
      const r=await fetchJson('/api/radar/ack',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({incident_id:selected.id})});
      if(!r.ok||!r.incident?.ack_at)throw new Error('Ciência não confirmada pelo servidor');
      selected=r.incident;
      if(data){const i=data.items.findIndex(x=>x.id===selected.id);if(i>=0)data.items[i]=selected;data.counts.ack_pending=Math.max(0,data.counts.ack_pending-1);}
      el('radarAckStatus').textContent=`Ciência registrada em ${when(selected.ack_at)}. Ver Análise liberado.`;
      render();
    }catch(e){el('radarAckStatus').textContent='Não foi possível gravar a ciência. Tente novamente. Ver Análise permanece bloqueado.';}
    finally{ackBusy=false;fillDialog();if(selected?.ack_at)el('radarAnalyze').focus();}
  }
  function closeAcknowledged(){
    if(!selected?.ack_at)return;
    el('radarAlert').close();selected=null;previousFocus?.focus?.();showPending();
  }
  async function analyze(){
    if(!selected?.ack_at)return;
    const x=selected;
    try{
      // Ask app.js to select the correct instance/queue through its existing D3 flow.
      if(typeof window.toiOpenRadarAnalysis!=='function')throw new Error('Painel ainda está carregando');
      el('radarAnalyze').disabled=true;
      await window.toiOpenRadarAnalysis(x);
      el('radarAlert').close();selected=null;
    }catch(e){el('radarAckStatus').textContent=e.message||'Análise indisponível';fillDialog();}
  }
  async function poll(){
    if(pollBusy)return;pollBusy=true;
    try{
      data=await fetchJson('/api/radar');render();
      if(selected&&!ackBusy){const latest=data.items?.find(x=>x.id===selected.id);if(latest){selected=latest;fillDialog();}}
      if(resolvedSelected&&!ackBusy){resolvedSelected=data.items?.find(x=>x.id===resolvedSelected.id)||resolvedSelected;}
      // Do not cover an analysis the operator deliberately opened.
      if(!document.querySelector('.analytics-modal.open,.infra-inventory-modal.open,#mlReportDialog[open],#changePasswordDialog[open]'))showPending();
      if(mode==='period')loadHistory(false);
    }catch(e){el('radarSource').textContent='Radar indisponível. Exibindo o último estado recebido.';el('radarSource').classList.add('warn');}
    finally{pollBusy=false;setTimeout(poll,5000);}
  }
  function init(){
    el('radarNowTab').onclick=()=>showMode('now');el('radarPeriodTab').onclick=()=>showMode('period');
    el('radarPause').setAttribute('aria-pressed',String(paused));el('radarPause').textContent=paused?'Retomar':'Pausar';
    el('radarPause').onclick=()=>{paused=!paused;el('radarPause').setAttribute('aria-pressed',String(paused));el('radarPause').textContent=paused?'Retomar':'Pausar';};
    const toggleSound=async()=>{
      try{
        if(!sound)sound=new (window.AudioContext||window.webkitAudioContext)();
        await sound.resume();soundEnabled=!soundEnabled;
        for(const id of ['radarSound','radarAlertSound']){el(id).textContent=soundEnabled?'Som ligado':'Ativar som';el(id).setAttribute('aria-pressed',String(soundEnabled));}
        if(soundEnabled){lastSoundId='';beep(selected?.id||'sound-enabled');}
      }catch(e){for(const id of ['radarSound','radarAlertSound'])el(id).textContent='Som indisponível';}
    };
    el('radarSound').onclick=toggleSound;el('radarAlertSound').onclick=toggleSound;
    el('radarExpand').onclick=()=>{el('radarExpandedTitle').textContent=mode==='now'?'Radar Operacional · todas as instâncias':`Radar Operacional · ${period?.label||'período'}`;el('radarExpanded').showModal();render();};
    el('radarExpandedClose').onclick=()=>{el('radarExpanded').close();el('radarExpand').focus();showPending();};
    el('radarAlert').addEventListener('cancel',e=>{e.preventDefault();if(selected?.ack_at)closeAcknowledged();});
    el('radarAcknowledge').onclick=acknowledge;el('radarAlertNext').onclick=closeAcknowledged;el('radarAnalyze').onclick=analyze;
    el('radarResolvedAcknowledge').onclick=acknowledgeResolution;
    el('radarResolved').addEventListener('cancel',e=>e.preventDefault());
    el('radarMore').onclick=()=>loadHistory(true);
    document.addEventListener('click',e=>{
      const b=e.target.closest('[data-radar-open],[data-radar-pending],[data-radar-more],[data-radar-history-retry]');if(!b)return;
      if(b.hasAttribute('data-radar-open'))openIncident(data?.items.find(x=>x.id===b.dataset.radarOpen));
      else if(b.hasAttribute('data-radar-pending')){if(el('radarExpanded').open)el('radarExpanded').close();showPending();}
      else if(b.hasAttribute('data-radar-more'))loadHistory(true);
      else{historyAt=0;loadHistory(false);}
    });
    for(const node of [el('radarNow'),el('radarPeriod')]){
      node.addEventListener('mouseenter',()=>hover=true);node.addEventListener('mouseleave',()=>hover=false);
    }
    for(const id of ['radarNowTab','radarPeriodTab'])el(id).addEventListener('keydown',e=>{
      if(!['ArrowRight','ArrowLeft','Home','End'].includes(e.key))return;e.preventDefault();showMode(mode==='now'?'period':'now');el(mode==='now'?'radarNowTab':'radarPeriodTab').focus();
    });
    setInterval(()=>{
      const node=el(mode==='now'?'radarNow':'radarPeriod');
      if(paused||hover||document.hidden||el('radarAlert').open||el('radarResolved').open||el('radarExpanded').open||node.contains(document.activeElement)||scrollRest>Date.now())return;
      if(node.scrollHeight<=node.clientHeight+2)return;
      if(node.scrollTop+node.clientHeight>=node.scrollHeight-1){scrollRest=Date.now()+3500;node.scrollTop=0;}
      else node.scrollTop+=1;
    },80);
    poll();
  }
  window.TOIRadar={setPeriod};
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init,{once:true});else init();
})();
