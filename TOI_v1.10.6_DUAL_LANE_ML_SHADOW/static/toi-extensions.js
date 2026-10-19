/* Páginas complementares do TOI antigo. */
(() => {
  const $ = id => document.getElementById(id);
  const html = value => String(value ?? '—').replace(/[&<>"']/g, char =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
  const number = value => value === null || value === undefined || value === '' ? '—' :
    Number(value).toLocaleString('pt-BR',{maximumFractionDigits:1});
  const percent = value => value === null || value === undefined ? '—' :
    Number(value).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2})+'%';
  const congestionCell = value => `<strong class="congestion-${Number(value)>=20?'critical':Number(value)>=10?'warning':'normal'}">${percent(value)}</strong>`;
  const scope = () => ({instance:$('instanceSelect')?.value || '',queue:$('queueSelect')?.value || ''});
  const currentData = () => typeof lastData === 'undefined' ? null : lastData;
  const currentAnalytics = () => typeof lastAnalytics === 'undefined' ? null : lastAnalytics;
  const queueCatalog = () => typeof queues === 'undefined' ? [] : queues;
  const selectedQueue = () => queueCatalog().find(q =>
    String(q.instancia) === scope().instance && String(q.codigo) === scope().queue);
  const formatTime = value => {
    const date = value ? new Date(value) : null;
    return date && !Number.isNaN(date.getTime()) ? date.toLocaleString('pt-BR') : '—';
  };
  async function getJson(url, options={}) {
    const response=await fetch(url,{cache:'no-store',credentials:'same-origin',signal:AbortSignal.timeout(12000),...options});
    const data=await response.json();
    if(!response.ok)throw new Error(data.error||`HTTP ${response.status}`);
    return data;
  }

  document.addEventListener('change',event=>{
    if(event.target?.id==='instanceSelect'||event.target?.id==='queueSelect'){
      setTimeout(()=>{if(activeView)renderSection(activeView);},150);
    }
  });

  const section=document.createElement('section');section.id='sectionView';section.hidden=true;
  section.setAttribute('aria-live','polite');document.querySelector('.main').append(section);
  const menu=[...document.querySelectorAll('.sidebar .nav')];
  const labels=menu.map(element=>element.querySelector('span')?.textContent.trim());
  let activeView='',adminSession=null,adminUsers=[],liveQueues=[],catalogQueues=[],sipSample=null,radarSample=null,incidentHistory=null;
  let infraSample=null,sectionRequest=0;
  const emptyExecutiveFilter=()=>({mode:'recent',date:'',instance:'',queue:'',source:'',state:''});
  let executiveEvents=[],executiveError='',executiveFilter=emptyExecutiveFilter(),executiveScopes=[];
  const executiveSources={PORTAL_ORBE:'Portal Orbe',GATEWAY:'Gateways',AGIROUTE:'Agiroute · Instância 6',MONITOR_ALERTAS:'Monitor de alertas · SBC/MGW',AUTO_LIMPEZA:'Limpeza automática de canais'};
  const tile=(label,value,note='')=>`<article class="section-tile"><h3>${html(label)}</h3><strong>${html(value)}</strong>${note?`<p>${html(note)}</p>`:''}</article>`;
  const table=(headers,rows)=>`<table class="section-table"><thead><tr>${headers.map(x=>`<th>${html(x)}</th>`).join('')}</tr></thead><tbody>
    ${rows.length?rows.map(row=>`<tr>${row.map(cell=>`<td>${cell}</td>`).join('')}</tr>`).join(''):`<tr><td colspan="${headers.length}">Sem registros disponíveis nesta seleção.</td></tr>`}</tbody></table>`;
  const action=(name,label)=>`<button type="button" class="section-action" data-section-action="${name}">${html(label)}</button>`;
  const eventRows=()=>((radarSample?.items)||[]).filter(x=>
    String(x.instancia)===scope().instance && (!x.codigo||String(x.codigo)===scope().queue)).map(x=>[
      html(formatTime(x.started_at)),html(x.entity),`<span class="incident-state ${x.active?'active':'resolved'}">${x.active?'Ativo':'Resolvido'}</span>`,
      html(x.source==='PORTAL_ORBE'?'Portal Orbe':'Gateway')]);
  const executiveStatus=x=>x.state||(['NORMALIZED','NORMALIZATION_OBSERVED','SUPERSEDED','HISTORY_RECONCILED'].includes(x.kind)?'Resolvido':
    ['OPENED','PERSISTENT','IMPORTED_ACTIVE'].includes(x.kind)?(x.ack_at?'Ativo e Ciente':'Ativo e pendência de Ciência'):
    x.kind==='AUTO_CLEANUP'?'Limpeza automática':x.kind==='OBSERVED'?'Observado':'Evento');
  const executiveResource=x=>x.kind==='AUTO_CLEANUP'?`${x.entity||'Limpeza automática de canais'} · ${x.channels_released===null||x.channels_released===undefined?'quantidade não registrada':`${number(x.channels_released)} ${Number(x.channels_released)===1?'canal limpo':'canais limpos'}`}`:x.entity||x.entity_key||'—';
  const executiveRows=()=>executiveEvents.map(x=>[
    `${html(formatTime(x.occurred_at||x.started_at))}${['activation','resolution'].includes(x.occurrence)?`<small class="occurrence-note">${x.occurrence==='activation'?'Ativação':'Resolução'}</small>`:''}${x.resolution_time_basis==='OPERATOR_RECONCILIATION'&&x.occurrence==='resolution'?'<small class="occurrence-note">Registro da correção histórica; horário da normalização não disponível</small>':''}`,
    html(x.instancia||'—'),html(x.codigo||'—'),html(executiveResource(x)),
    `<span class="incident-state ${executiveStatus(x)==='Resolvido'?'resolved':executiveStatus(x).startsWith('Ativo')?'active':''}">${html(executiveStatus(x))}</span>`,
    html(x.source||'—')]);
  const executiveFiltersHtml=()=>{
    const instances=[...new Set([...catalogQueues,...executiveScopes].map(x=>String(x.instancia)).filter(Boolean))].sort((a,b)=>a.localeCompare(b,'pt-BR',{numeric:true}));
    const queuesInInstance=executiveQueueOptions(executiveFilter.instance);
    return `<form id="executiveIncidentFilter" class="section-wide executive-filter">
      <label>Data (opcional)<input name="date" type="date" value="${html(executiveFilter.date)}"></label>
      <label>Instância<select name="instance"><option value="">Todas as instâncias</option>${instances.map(x=>`<option value="${html(x)}" ${x===executiveFilter.instance?'selected':''}>${html(x)}</option>`).join('')}</select></label>
      <label>Fila<select name="queue" ${executiveFilter.instance?'':'disabled'}><option value="">Todas as filas</option>${queuesInInstance.map(x=>`<option value="${html(x.codigo)}" ${String(x.codigo)===executiveFilter.queue?'selected':''}>${html(x.codigo)} · ${html(x.nome||x.codigo)}</option>`).join('')}</select></label>
      <label>Origem<select name="source"><option value="">Todas as origens</option>${Object.entries(executiveSources).map(([value,label])=>`<option value="${value}" ${value===executiveFilter.source?'selected':''}>${html(label)}</option>`).join('')}</select></label>
      <label>Estado<select name="state"><option value="">Todos os estados</option>${['Ativo e Ciente','Ativo e pendência de Ciência','Resolvido','Observado','Limpeza automática','Evento'].map(x=>`<option value="${html(x)}" ${x===executiveFilter.state?'selected':''}>${html(x)}</option>`).join('')}</select></label>
      <button class="section-action" type="submit">Pesquisar</button>${action('executive-recent','Últimos 2 dias')}${action('executive-clear','Limpar filtros')}
      <small class="section-wide">Sem data: últimas 48 horas. Selecione uma data para consultar o histórico daquele dia. A fila é opcional.</small></form>`;
  };
  function executiveQueueOptions(instance){
    if(!instance)return [];
    const options=new Map();
    for(const item of [...catalogQueues,...executiveScopes]){
      if(String(item.instancia)===instance&&item.codigo&&!options.has(String(item.codigo)))options.set(String(item.codigo),item);
    }
    return [...options.values()].sort((a,b)=>String(a.codigo).localeCompare(String(b.codigo)));
  }
  const readExecutiveFilter=form=>({mode:form.elements.date.value?'date':'recent',date:form.elements.date.value,
    instance:form.elements.instance.value,queue:form.elements.queue.value,source:form.elements.source.value,state:form.elements.state.value});
  const executiveScopeNote=()=>[executiveFilter.mode==='recent'?'Últimas 48 horas':`Dia ${executiveFilter.date}`,
    executiveFilter.instance?`Instância ${executiveFilter.instance}`:'todas as instâncias',
    executiveFilter.queue?`fila ${executiveFilter.queue}`:'todas as filas',executiveSources[executiveFilter.source],executiveFilter.state].filter(Boolean).join(' · ');
  async function loadExecutiveHistory(request){
    const filter={...executiveFilter},events=[];let scopes=executiveScopes,errorMessage='';
    const end=filter.mode==='recent'?new Date():new Date(filter.date+'T23:59:59.999-03:00');
    const start=filter.mode==='recent'?new Date(end.getTime()-48*3600000):new Date(filter.date+'T00:00:00-03:00');
    if(Number.isNaN(+start)||Number.isNaN(+end)){if(request===sectionRequest)executiveError='Data inválida.';return;}
    try{
      let cursor=null;const seen=new Set();
      do{
        const query=new URLSearchParams({start:start.toISOString(),end:end.toISOString(),exact_scope:'1',occurrences:'1'});
        if(filter.instance)query.set('instancia',filter.instance);
        if(filter.queue)query.set('codigo',filter.queue);
        if(filter.source)query.set('source',filter.source);
        if(cursor)query.set('before',cursor);
        const page=await getJson('/api/radar/history?'+query);
        if(request!==sectionRequest)return;
        if(page.scopes)scopes=page.scopes;
        for(const item of page.events||[]){
          if(filter.instance&&String(item.instancia)!==filter.instance)continue;
          if(filter.queue&&String(item.codigo)!==filter.queue)continue;
          if(filter.source&&item.source!==filter.source)continue;
          if(filter.state&&executiveStatus(item)!==filter.state)continue;
          if(!seen.has(item.id)){seen.add(item.id);events.push(item);}
        }
        cursor=page.next_cursor;
      }while(cursor);
      events.sort((a,b)=>Date.parse(b.occurred_at)-Date.parse(a.occurred_at));
    }catch(error){errorMessage=error.message;}
    if(request===sectionRequest){executiveEvents=events;executiveScopes=scopes;executiveError=errorMessage;}
  }
  const oldEventRows=()=>((currentData()?.recent_incidents)||[]).filter(x=>
    String(x?.status||'').toUpperCase()==='ATIVO'||
    Date.parse(x?.resolved_at||x?.end||x?.start||'')>=Date.now()-24*60*60*1000).map(x=>[
    html(formatTime(x.start)),html(x.type),`<span class="incident-state ${String(x.status).toUpperCase()==='ATIVO'?'active':'resolved'}">${String(x.status).toUpperCase()==='ATIVO'?'Ativo':'Resolvido'}</span>`,html(x.max===null?'—':percent(x.max))]);
  const inventory=(type)=>((infraSample?.instance===scope().instance?infraSample:null)?.[type]||[]).slice()
    .sort((a,b)=>(Number(b.occupancy)||0)-(Number(a.occupancy)||0));
  const fresh=()=>currentData()?.freshness?.sources||{};

  function sectionContent(name) {
    const d=currentData()||{},a=currentAnalytics()||{},l=d.latest||{},dial=d.dialer||{},q=queueCatalog();
    const shown=q.filter(x=>String(x.instancia)===scope().instance);
    const queueRows=shown.map(x=>[html(x.nome),html(x.codigo),
      `<button class="section-action" data-choose-queue="${html(x.codigo)}">Selecionar</button>`]);
    const entries=(values)=>Object.entries(values).map(([label,value])=>[html(label),html(value)]);
    switch(name) {
      case 'Alertas e Incidentes':
        return `${tile('Ocorrências',executiveEvents.length,executiveScopeNote())}
          ${tile('Ativações',executiveEvents.filter(x=>x.occurrence==='activation').length)}${tile('Resoluções',executiveEvents.filter(x=>x.occurrence==='resolution').length)}
          ${executiveFiltersHtml()}${executiveError?`<p class="section-wide section-error">${html(executiveError)}</p>`:''}
          ${table(['Ocorrência','Instância','Fila','Recurso','Estado','Origem'],executiveRows())}`;
      case 'Incidentes':
        return `${tile('Eventos no período',incidentHistory?.events?.length??0,'Histórico preservado')}${tile('Fila',selectedQueue()?.nome||scope().queue)}${tile('Qualidade da fila',fresh().queue?.status||'N/C')}
          ${table(['Ocorrência','Fila / Recurso','Transição','Origem'],(incidentHistory?.events||[]).map(x=>[
            html(formatTime(x.occurred_at)),html(x.entity),html(({OPENED:'Ativo',NORMALIZED:'Resolvido',ACKNOWLEDGED:'Ciente'})[x.kind]||x.kind),html(x.source)]))}`;
      case 'Resumo por Instância':
        {
          const catalog=catalogQueues.length?catalogQueues:q;
          const instances=[...new Set(catalog.map(x=>String(x.instancia)))].sort((a,b)=>a.localeCompare(b,'pt-BR'));
          const sampled=new Set(liveQueues.map(x=>String(x.instancia)+'|'+String(x.codigo)));
          return `${tile('Filas no catálogo',catalog.length,'Todas as filas cadastradas')}${tile('Com amostra LIVE',sampled.size)}${tile('Instâncias',instances.length)}
            <div class="section-wide instance-summary">${instances.map(inst=>{
              const group=catalog.filter(x=>String(x.instancia)===inst);
              const count=group.filter(x=>sampled.has(inst+'|'+String(x.codigo))).length;
              return `<details class="instance-group"><summary><strong>Instância ${html(inst)}</strong><span>${number(group.length)} filas · ${number(count)} com amostra LIVE</span></summary>
                ${table(['Fila','Código','Amostra'],group.map(x=>[html(x.nome),html(x.codigo),html(sampled.has(inst+'|'+String(x.codigo))?'LIVE':'Sem amostra atual')]))}</details>`;
            }).join('')}</div>`;
        }
      case 'Filas':
        return `${tile('Filas desta instância',shown.length)}${tile('P34 · fila em foco',percent(l.p34_pct))}${tile('P135 · fila em foco',percent(l.p135_pct))}
          ${table(['Fila','Código','P34','P135','Discando','Em atendimento','Atualização'],liveQueues.filter(x=>String(x.instancia)===scope().instance).map(x=>[
            html(x.nome),html(x.codigo),congestionCell(x.p34_pct),congestionCell(x.p135_pct),number(x.calls_dialing),number(x.calls_in_attendance),html(formatTime(x.ts))]))}`;
      case 'Agentes':
        return `${tile('Disponíveis',number(l.agents_available))}${tile('Falando',number(l.agents_talking))}${tile('ACW',number(l.agents_acw))}
          ${table(['Fila','Disponíveis','Falando','ACW','Pausados','Outros','Discando','Atualização'],liveQueues.filter(x=>String(x.instancia)===scope().instance).map(x=>[
            html(x.nome),number(x.agents_available),number(x.agents_talking),number(x.agents_acw),number(x.agents_paused),number(x.agents_other),number(x.agents_dialing),html(formatTime(x.ts))]))}`;
      case 'Discador':
        return `${tile('Chamadas · 5 min',number(dial.calls5))}${tile('Chamadas · 15 min',number(dial.calls15))}${tile('Buffer',number(dial.buffer))}
          ${table(['Indicador','Valor'],entries({'Campanhas':number(dial.campaigns),'Hit Rate 5′':percent(l.hit_rate5),'Hit Rate 15′':percent(l.hit_rate15),'Tentativas finalizadas':number(dial.finalized_total)}))}`;
      case 'Mailing & Campanhas':
        return `${tile('Campanhas',number(dial.campaigns))}${tile('Mailing disponível',number(dial.mailing_available))}${tile('Na tela',number(dial.mailing_on_screen))}
          ${table(['Indicador','Valor'],entries({'Mailing total':number(dial.mailing_total),'Mailing livre':number(dial.mailing_free),'Bloqueios diários':number(dial.blocked_attempts_daily)}))}`;
      case 'Gateways':
      case 'Grupos de Troncos': {
        const gateway=name==='Gateways',items=inventory(gateway?'gateways':'trunks');
        const source=infraSample?.sources?.[gateway?'gateways':'trunks'];
        return `${tile('Recursos observados',source?.sample_at?items.length:'—')}${tile('Mais ocupado',items[0]?percent(items[0].occupancy):'—')}${tile('Fonte Infra',infraSample?.error?'INDISPONÍVEL':source?.status||'AGUARDANDO',source?.sample_at?'Leitura '+formatTime(source.sample_at):'Aguardando leitura da instância')}
          ${infraSample?.error?`<p class="section-error section-wide">${html(infraSample.error)} · última amostra preservada.</p>`:''}
          <div class="section-wide section-actions">${action(gateway?'gateway-analysis':'trunk-analysis','Analisar no período')}</div>
          ${table([gateway?'Gateway':'Grupo de Troncos','Ocupação','Ocupados','Total','Livres','Agora'],items.map(x=>[html(x.entity),percent(x.occupancy),number(x.busy),number(x.total),
            Number.isFinite(Number(x.total))&&Number.isFinite(Number(x.busy))?number(Math.max(0,Number(x.total)-Number(x.busy))):'—',
            Number(x.busy)>0?'<span class="traffic-up">Com tráfego</span>':'<span class="traffic-down">Sem tráfego</span>']))}`;
      }
      case 'Canais Travados':
        return `${tile('Canais travados agora',number(d.stuck?.quantity))}${tile('Fonte',fresh().stuck_channels?.status||'N/C')}
          <div class="section-wide section-actions">${action('stuck-analysis','Abrir análise de canais')}</div>`;
      case 'SIP Servers':
        return `${tile('Servidores SIP',sipSample?.sample_at?sipSample.items.length:'—')}${tile('Fonte',sipSample?.status||'AGUARDANDO','Portal Orbe · por instância')}${tile('Última leitura',formatTime(sipSample?.sample_at),sipSample?.age_s==null?'':number(sipSample.age_s)+' s atrás')}
          ${sipSample?.error?`<p class="section-error section-wide">${html(sipSample.error)}</p>`:''}
          ${table(['SIP Server','Agentes','Agentes distintos','Capacidade de agentes','Ocupação'],(sipSample?.items||[]).map(x=>[html(x.server),number(x.agents),number(x.distinct_agents),number(x.capacity),percent(x.occupancy)]))}`;
      case 'Diagnóstico Automático':
        return `${tile('Diagnóstico observado',d.diagnosis?.title||'N/C')}${tile('Severidade',d.diagnosis?.severity||'N/C')}${tile('Confiança',percent(d.diagnosis?.confidence))}
          <article class="section-tile section-wide"><h3>Qualidade e governança</h3><p>${html((d.diagnosis?.quality_notes||[]).join(' · ')||'Sem observações adicionais.')}</p></article>
          ${tile('Modelo analítico',d.ml_shadow?.model_version||'Não disponível','Modo SHADOW · sem execução automática')}
          ${tile('Risco observado',d.ml_shadow?.risk_score===undefined?'—':percent(d.ml_shadow.risk_score),d.ml_shadow?.risk_class||'Sem classificação')}
          ${tile('Base recente',d.ml_shadow?.features?.samples??'—','Amostras; modelo não homologado para produção')}
          <article class="section-tile section-wide"><h3>Fatores observados</h3><p>${html(d.ml_shadow?.explanation||'Sem fatores suficientes.')}</p></article>`;
      case 'Correlações':
        return `<article class="section-tile section-wide"><h3>Relações observadas no período</h3><p>Relações operacionais não estabelecem causalidade.</p></article>
          ${table(['Indicador','Valor'],Object.entries(a.operational_pressure?.relations||{}).map(([key,value])=>[html(key.replaceAll('_',' ')),html(typeof value==='number'?number(value):value)]))}`;
      case 'Padrões & Recorrências':
        return `${tile('Fatos destacados',a.event_intelligence?.highlights?.length||0)}${tile('Incidentes recentes',eventRows().length)}
          ${table(['Horário','Fato','Valor'],(a.event_intelligence?.highlights||[]).map(x=>[html(formatTime(x.ts)),html(x.label),html(x.value)]))}`;
      case 'Análise de Capacidade': {
        const c=a.capacity_analytics?.operational||d.capacity||{};
        return `${tile('Ocupação atual',percent(c.current))}${tile('Ocupação que 95% das leituras não ultrapassaram',percent(c.p95))}${tile('Folga no nível que 95% das leituras não ultrapassaram',percent(c.headroom_p95))}
          <div class="section-wide section-actions">${action('capacity-analysis','Abrir análise completa')}</div>`;
      }
      case 'Recomendações': {
        const list=a.operational_recommendations||d.recommendations||[];
        return `${tile('Recomendações observadas',list.length)}${table(['Recomendação','Prioridade','Justificativa'],list.map(x=>[html(x.title||x.name||x.text),html(x.priority||x.severity),html(x.reason||x.description||x.impact)]))}`;
      }
      case 'Comparativo': {
        const labels={'p34_pct':'P34','p135_pct':'P135','hit_rate5':'Hit Rate 5′','hit_rate15':'Hit Rate 15′','calls_dialing':'Discando','calls_in_attendance':'Em atendimento'};
        return `${tile('Período',a.period_label||'15 min')}${tile('Amostras',a.coverage?.queue?.samples??'—')}${tile('Cobertura da fila',percent(a.coverage?.queue?.data_coverage_pct))}
          ${table(['Métrica','Agora','Média','Mínimo','95% das leituras ficaram até','Máximo','Variação da amostra'],Object.entries(labels).map(([key,label])=>{
          const row=a.queue_summary?.[key]||{};const pc=key.includes('pct')||key.includes('rate');const show=v=>pc?percent(v):number(v);
          return [html(label),show(row.current??l[key]),show(row.mean),show(row.min),show(row.p95),show(row.max),show(row.delta)];}))}`;
      }
      case 'Exportações':
        return `<article class="section-tile section-wide"><h3>Exportação da seleção atual</h3><p>${html(a.period_label||'15 min')} · série temporal da fila selecionada; sem telefones, credenciais ou JSON bruto. Respeita o período escolhido.</p>
          ${action('export-csv','Baixar série temporal em CSV')}</article>`;
      case 'Instâncias':
        return `${tile('Instâncias observadas',new Set(q.map(x=>x.instancia)).size)}${tile('Instância selecionada',scope().instance)}${tile('Fonte','Portal Orbe · somente leitura')}
          ${table(['Instância','Filas monitoradas','Última amostra'],[...new Set(q.map(x=>x.instancia))].map(inst=>[
            html(inst),number(q.filter(x=>x.instancia===inst).length),html(formatTime(liveQueues.filter(x=>String(x.instancia)===String(inst)).map(x=>x.ts).sort().at(-1)))]))}`;
      case 'Sobre o Sistema':
        return `${tile('Versão','TOI v1.10.6')}${tile('Fonte','Portal Orbe · LIVE')}${tile('IPBX','Relatório sob demanda · Trace de Chamadas')}`;
      default:return '<article class="section-tile section-wide"><p>Seleção sem dados.</p></article>';
    }
  }

  function shell(name,content) {
    const searchable=['Resumo por Instância','Filas','Agentes','Gateways','Grupos de Troncos','SIP Servers','Incidentes','Alertas e Incidentes','Usuários','Instâncias'].includes(name);
    const perInstance=['Gateways','Grupos de Troncos','SIP Servers'].includes(name);
    const filter=section.querySelector('#sectionSearch')?.value||'';
    section.innerHTML=`<div class="section-view-head"><div><h2>${html(name)}</h2><small>${name==='Alertas e Incidentes'?html(executiveScopeNote()):'Instância '+html(scope().instance)+' · '+(perInstance?'Todas as filas':html(selectedQueue()?.nome||scope().queue)+' · '+html($('updatedAt')?.textContent))}</small></div>
      <button type="button" data-section-action="overview">Voltar à Visão Geral</button></div><div class="section-view-body">${searchable?'<label class="section-search section-wide">Pesquisar nesta lista<input id="sectionSearch" type="search" placeholder="Filtrar registros…"></label>':''}${content}</div>`;
    if(filter&&$('sectionSearch')){$('sectionSearch').value=filter;$('sectionSearch').dispatchEvent(new Event('input',{bubbles:true}));}
  }
  async function renderSection(name) {
    const request=++sectionRequest,selected=scope();
    if(name==='Usuários'||name==='Configurações')return renderAdmin(name);
    if(['Resumo por Instância','Filas','Agentes','Instâncias'].includes(name)){
      try { liveQueues=(await getJson('/api/queues/live')).items||[]; } catch(_) { liveQueues=[]; }
    }
    if(['Resumo por Instância','Alertas e Incidentes'].includes(name)){
      try { catalogQueues=await getJson('/api/queues'); } catch(_) { catalogQueues=queueCatalog(); }
    }
    if(name==='SIP Servers'){
      if(sipSample?.instance!==selected.instance)sipSample=null;
      try {
        const result=await getJson(`/api/sip-servers?instancia=${encodeURIComponent(selected.instance)}`);
        if(request!==sectionRequest)return;sipSample=result;
      }catch(error){if(request!==sectionRequest)return;sipSample={...sipSample,error:error.message,status:'INDISPONÍVEL'};}
    }
    if(['Gateways','Grupos de Troncos'].includes(name)){
      if(infraSample?.instance!==selected.instance)infraSample=null;
      try {
        const result=await getJson(`/api/infrastructure?instancia=${encodeURIComponent(selected.instance)}`);
        if(request!==sectionRequest)return;infraSample=result;
      }catch(error){if(request!==sectionRequest)return;infraSample={...infraSample,error:error.message};}
    }
    if(['Alertas e Incidentes','Incidentes'].includes(name)){
      try { radarSample=await getJson('/api/radar'); } catch(_) { radarSample=null; }
      if(name==='Alertas e Incidentes')await loadExecutiveHistory(request);
      if(name==='Incidentes'){
        const s=scope(),end=new Date(),start=new Date(end.getTime()-24*3600000);
        try { incidentHistory=await getJson(`/api/radar/history?start=${encodeURIComponent(start.toISOString())}&end=${encodeURIComponent(end.toISOString())}&instancia=${encodeURIComponent(s.instance)}&codigo=${encodeURIComponent(s.queue)}`); }
        catch(_) { incidentHistory=null; }
      }
    }
    if(request===sectionRequest&&activeView===name)shell(name,sectionContent(name));
  }
  function openSection(name) {
    if(name==='Alertas e Incidentes')executiveFilter=emptyExecutiveFilter();
    activeView=name==='Visão Geral'?'':name;
    section.hidden=!activeView;document.querySelector('.main').classList.toggle('section-open',Boolean(activeView));
    menu.forEach(nav=>nav.classList.toggle('active',nav.querySelector('span')?.textContent.trim()===name));
    if(activeView)renderSection(name);
  }
  window.toiOpenSection=openSection;
  window.addEventListener('toi-selection-ready',()=>{if(activeView)renderSection(activeView);});
  setInterval(()=>{if(window.toiAuthenticated&&!document.hidden&&['Gateways','Grupos de Troncos','SIP Servers'].includes(activeView))renderSection(activeView);},15000);
  menu.forEach((nav,index)=>{
    const name=labels[index];if(!name)return;
    nav.classList.remove('muted');nav.dataset.section=name;nav.tabIndex=0;nav.setAttribute('role','button');
    nav.addEventListener('click',event=>{event.preventDefault();event.stopImmediatePropagation();openSection(name);},true);
    nav.addEventListener('keydown',event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();nav.click();}});
  });
  document.querySelector('.sidebar .about')?.addEventListener('click',()=>openSection('Sobre o Sistema'));

  function loginForm(error='') {
    return `<article class="section-tile section-wide"><h3>Acesso administrativo</h3><p>Use sua conta do TOI antigo. A administração é restrita ao computador local.</p>
      <form id="oldAdminLogin" class="section-form"><label>Login<input name="login" autocomplete="username" required></label>
      <label>Senha<input name="password" type="password" autocomplete="current-password" required></label>
      <button class="section-action" type="submit">Entrar</button></form><p class="section-error">${html(error)}</p></article>`;
  }
  async function adminApi(path,body) {
    return getJson('/api/admin/'+path,body===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':adminSession?.csrf||''},body:JSON.stringify(body)});
  }
  async function renderAdmin(name) {
    shell(name,loginForm());
    try {
      adminSession=await adminApi('me');
      if(adminSession.user.role!=='Administrador')throw new Error('Acesso de administrador necessário');
      if(name==='Usuários') {
        const result=await adminApi('users');
        adminUsers=result.items;
        const rows=result.items.map(user=>[html(user.login),html(user.name),html(user.role),html(JSON.parse(user.instances).join(', ')),
          user.active?'Ativo':'Inativo',`<button class="section-action" data-admin-user="${user.id}">Gerenciar</button>`]);
        shell(name,`${tile('Usuários cadastrados',result.items.length)}<div class="section-wide section-actions">${action('admin-logout','Sair do TOI')}</div>
          ${table(['Login','Nome','Perfil','Instâncias','Estado','Ação'],rows)}
          <div class="section-wide section-actions">${action('admin-audit','Ver auditoria de acessos e alterações')}</div><div id="adminAudit" class="section-wide"></div>
          <article class="section-tile section-wide"><h3>Criar usuário</h3><form id="oldAdminCreate" class="section-form">
            <label>Login<input name="login" required maxlength="120"></label><label>Nome<input name="name" required maxlength="120"></label>
            <label>Perfil<select name="role"><option>Consulta</option><option>Operador</option><option>Gestor</option><option>Administrador</option></select></label>
            <label>Instâncias (* ou códigos separados por vírgula)<input name="instances" value="*" required></label>
            <label>Senha inicial (mínimo 10; maiúscula, minúscula, número e especial)<input name="password" type="password" minlength="10" required></label>
            <button class="section-action" type="submit">Criar usuário</button></form><p id="oldAdminMessage"></p></article>`);
      } else {
        const settings=await adminApi('settings'),win=settings.operational_window||{};
        shell(name,`<article class="section-tile section-wide"><h3>Janela operacional padrão</h3><p>Esta configuração altera a interpretação operacional do TOI antigo. Exceções por fila existentes permanecem preservadas.</p>
          <form id="oldAdminSettings" class="section-form"><label>Ativa<input name="enabled" type="checkbox" ${win.enabled?'checked':''}></label>
          <label>Início<input name="start_time" type="time" value="${html(win.start_time)}" required></label>
          <label>Fim<input name="end_time" type="time" value="${html(win.end_time)}" required></label>
          <button class="section-action" type="submit">Salvar configuração</button></form><p id="oldAdminMessage"></p></article>
          ${tile('IPBX',settings.ipbx_schedule,`Até ${settings.ipbx_limit_per_queue_status} números distintos por arquivo`)}
          ${tile('Exceções por fila',settings.queue_overrides)}`);
      }
    } catch(error) {adminSession=null;shell(name,loginForm(error.message));}
  }

  section.addEventListener('click',async event=>{
    const queue=event.target.closest('[data-choose-queue]');
    if(queue){$('queueSelect').value=queue.dataset.chooseQueue;await loadNow(true);renderSection(activeView);return;}
    const userButton=event.target.closest('[data-admin-user]');
    if(userButton){
      const user=adminUsers.find(item=>String(item.id)===userButton.dataset.adminUser);
      if(!user)return;
      $('oldAdminManageTitle').textContent='Gerenciar '+user.login;
      const form=$('oldAdminManageForm');form.dataset.userId=user.id;
      form.elements.action.value='revoke';form.elements.role.value=user.role;
      form.elements.instances.value=JSON.parse(user.instances).join(', ');
      form.elements.active.checked=Boolean(user.active);form.elements.password.value='';
      $('oldAdminManageMessage').textContent='';adminManage.showModal();
      return;
    }
    const button=event.target.closest('[data-section-action]');if(!button)return;
    const actionName=button.dataset.sectionAction;
    if(actionName==='overview'){openSection('Visão Geral');return;}
    if(actionName==='admin-audit'){
      try{const records=(await adminApi('audit')).items||[];
        $('adminAudit').innerHTML=table(['Quando','Usuário','Ação','Alvo'],records.map(x=>[html(formatTime(x.at)),html(x.user),html(x.action),html(x.target)]));}
      catch(error){$('adminAudit').textContent=error.message;}
      return;
    }
    if(actionName==='admin-logout'){try{await adminApi('logout',{});}finally{adminSession=null;location.reload();}return;}
    if(actionName==='gateway-analysis')openEntityPeriodAnalytics('gateways');
    if(actionName==='trunk-analysis')openEntityPeriodAnalytics('trunks');
    if(actionName==='stuck-analysis')openManagerialAnalytics('stuck');
    if(actionName==='capacity-analysis')openManagerialAnalytics('capacity');
    if(actionName==='incident-history')openIncidentHistory();
    if(actionName==='executive-recent'){
      executiveFilter={...readExecutiveFilter($('executiveIncidentFilter')),mode:'recent',date:''};await renderSection('Alertas e Incidentes');return;
    }
    if(actionName==='executive-clear'){
      executiveFilter=emptyExecutiveFilter();await renderSection('Alertas e Incidentes');return;
    }
    if(actionName==='export-csv')downloadCsv();
  });
  section.addEventListener('input',event=>{
    if(event.target.name==='instance'&&event.target.form?.id==='executiveIncidentFilter'){
      const select=event.target.form.elements.queue;
      select.disabled=!event.target.value;
      select.innerHTML='<option value="">Todas as filas</option>'+executiveQueueOptions(event.target.value)
        .map(x=>`<option value="${html(x.codigo)}">${html(x.codigo)} · ${html(x.nome||x.codigo)}</option>`).join('');
      return;
    }
    if(event.target.id!=='sectionSearch')return;
    const query=event.target.value.trim().toLocaleLowerCase('pt-BR');
    section.querySelectorAll('.section-table tbody tr').forEach(row=>{
      row.hidden=Boolean(query)&&!row.textContent.toLocaleLowerCase('pt-BR').includes(query);
    });
    section.querySelectorAll('.instance-group').forEach(group=>{
      const rows=[...group.querySelectorAll('tbody tr')];
      const summaryMatches=Boolean(query)&&group.querySelector('summary').textContent.toLocaleLowerCase('pt-BR').includes(query);
      if(summaryMatches)rows.forEach(row=>row.hidden=false);
      group.hidden=Boolean(query)&&!rows.some(row=>!row.hidden)&&!summaryMatches;
      if(query&&!group.hidden)group.open=true;
    });
  });
  section.addEventListener('submit',async event=>{
    if(event.target.id==='executiveIncidentFilter'){
      event.preventDefault();const form=event.target;
      executiveFilter=readExecutiveFilter(form);
      await renderSection('Alertas e Incidentes');return;
    }
    const form=event.target;if(!['oldAdminLogin','oldAdminCreate','oldAdminSettings'].includes(form.id))return;
    event.preventDefault();const fields=new FormData(form);
    try {
      if(form.id==='oldAdminLogin'){
        adminSession=await getJson('/api/admin/login',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({login:fields.get('login'),password:fields.get('password')})});
      } else if(form.id==='oldAdminCreate'){
        await adminApi('users',{login:fields.get('login'),name:fields.get('name'),role:fields.get('role'),
          password:fields.get('password'),instances:String(fields.get('instances')).split(',').map(x=>x.trim()).filter(Boolean)});
      } else {
        await adminApi('settings',{operational_window:{enabled:fields.get('enabled')==='on',
          start_time:fields.get('start_time'),end_time:fields.get('end_time')}});
      }
      await renderAdmin(activeView);
    } catch(error) {
      const place=$('oldAdminMessage')||form.parentElement.querySelector('.section-error');
      if(place){place.textContent=error.message;place.className='section-error';}
    }
  });

  const adminManage=document.createElement('dialog');adminManage.id='oldAdminManage';
  adminManage.innerHTML=`<div class="ipbx-dialog-head"><h2 id="oldAdminManageTitle">Gerenciar usuário</h2><button type="button" id="oldAdminManageClose">Fechar ×</button></div>
    <div class="ipbx-dialog-body"><form id="oldAdminManageForm" class="section-form">
    <label>Ação<select name="action"><option value="revoke">Revogar sessões</option><option value="active">Ativar / desativar</option><option value="reset">Redefinir senha</option><option value="permissions">Alterar perfil e instâncias</option><option value="delete">Remover definitivamente</option></select></label>
    <label>Conta ativa<input name="active" type="checkbox"></label>
    <label>Nova senha inicial (mínimo 10; maiúscula, minúscula, número e especial)<input name="password" type="password" minlength="10" autocomplete="new-password"></label>
    <label>Perfil<select name="role"><option>Consulta</option><option>Operador</option><option>Gestor</option><option>Administrador</option></select></label>
    <label>Instâncias (* ou códigos separados por vírgula)<input name="instances"></label>
    <button type="submit" class="section-action">Aplicar ação</button></form><p id="oldAdminManageMessage" role="status"></p></div>`;
  adminManage.className='admin-manage-dialog';document.body.append(adminManage);
  $('oldAdminManageClose').onclick=()=>adminManage.close();
  $('oldAdminManageForm').addEventListener('submit',async event=>{
    event.preventDefault();const form=event.target,fields=new FormData(form),actionName=fields.get('action');
    const body={action:actionName};
    if(actionName==='active')body.active=fields.get('active')==='on';
    if(actionName==='reset')body.password=fields.get('password');
    if(actionName==='permissions'){
      body.role=fields.get('role');body.instances=String(fields.get('instances')).split(',').map(x=>x.trim()).filter(Boolean);
    }
    if(actionName==='delete'&&!confirm('Remover definitivamente este usuário? O acesso e as sessões serão excluídos.'))return;
    try{await adminApi('users/'+form.dataset.userId,body);adminManage.close();await renderAdmin('Usuários');}
    catch(error){$('oldAdminManageMessage').textContent=error.message;$('oldAdminManageMessage').className='section-error';}
  });

  function downloadCsv() {
    const fields=['ts','p34_pct','p135_pct','hit_rate5','hit_rate15','calls_dialing','calls_in_attendance'];
    const rows=currentAnalytics()?.series?.queue||[];
    const cell=value=>`"${String(value??'').replaceAll('"','""')}"`;
    const csv=fields.join(';')+'\r\n'+rows.map(row=>fields.map(field=>cell(row[field])).join(';')).join('\r\n');
    const url=URL.createObjectURL(new Blob(['\ufeff'+csv],{type:'text/csv;charset=utf-8'}));
    const link=document.createElement('a');link.href=url;link.download='toi-indicadores-'+scope().instance+'-'+scope().queue+'.csv';link.click();
    setTimeout(()=>URL.revokeObjectURL(url),1000);
  }
})();
