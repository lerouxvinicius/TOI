/* Busca chamadas por Status IPBX e valida as operadoras sob demanda. */
(() => {
  const buttons=[...document.querySelectorAll('[data-blocked-queue]')];
  const message=document.getElementById('traceMessage');
  const summary=document.getElementById('blockedCarrierSummary');
  const storageKey='toiBlockedCallsJob';
  const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
  const statusLabels={'21':'Rel21 - Call Rejected','16':'Rel16 - Normal Call Clearing','34':'Rel34 - No Circuit Available'};
  const statusLabel=value=>statusLabels[String(value||'21')]||'Status IPBX desconhecido';
  let selectedQueue='',sound=null;
  const formDialog=document.createElement('dialog');
  formDialog.className='blocked-calls-dialog';
  formDialog.innerHTML=`<form method="dialog" id="blockedCallsForm"><h2 id="blockedCallsTitle">Busca de chamadas</h2>
    <p>Informe as datas e os horários como no Portal Orbe. Ao concluir, a planilha será baixada pelo navegador.</p>
    <div class="blocked-period-fields"><label>Data Início (dd/mm/aaaa)<input name="startDate" inputmode="numeric" placeholder="dd/mm/aaaa" maxlength="10" required></label>
    <label>Horário Início<input name="startTime" type="time" step="1" required></label>
    <label>Data Fim (dd/mm/aaaa)<input name="endDate" inputmode="numeric" placeholder="dd/mm/aaaa" maxlength="10" required></label>
    <label>Horário Fim<input name="endTime" type="time" step="1" required></label>
    <label class="ipbx-status-field">Status IPBX<select name="ipbxStatus" required><option value="21">Rel21 - Call Rejected</option><option value="16">Rel16 - Normal Call Clearing</option><option value="34">Rel34 - No Circuit Available</option></select></label></div>
    <p id="blockedPeriodError" class="section-error" role="alert"></p>
    <div class="blocked-dialog-actions"><button type="button" id="blockedCancel">Cancelar</button><button type="submit">Iniciar consulta</button></div></form>`;
  document.body.append(formDialog);
  formDialog.querySelector('#blockedCancel').onclick=()=>formDialog.close();
  const doneDialog=document.createElement('dialog');
  doneDialog.className='blocked-calls-dialog';
  doneDialog.innerHTML='<h2>Consulta concluída</h2><p id="blockedDoneText"></p><div class="blocked-dialog-actions"><button type="button" id="blockedDoneClose">Fechar</button></div>';
  document.body.append(doneDialog);
  doneDialog.querySelector('#blockedDoneClose').onclick=()=>doneDialog.close();
  function dateText(date){return `${String(date.getDate()).padStart(2,'0')}/${String(date.getMonth()+1).padStart(2,'0')}/${date.getFullYear()}`;}
  function validStamp(date,time){
    const m=/^(\d{2})\/(\d{2})\/(\d{4})$/.exec(date),t=/^(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(time);
    if(!m||!t)return null;
    const day=+m[1],month=+m[2],year=+m[3],hour=+t[1],minute=+t[2],second=+(t[3]||'00');
    const d=new Date(year,month-1,day,hour,minute,second);
    if(d.getFullYear()!==year||d.getMonth()!==month-1||d.getDate()!==day||hour>23||minute>59||second>59)return null;
    return {value:`${date} ${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')}:${String(second).padStart(2,'0')}`,epoch:d.getTime()};
  }
  function notifyDone(queue,period,ipbxStatus){
    const phrase=`Consulta de chamadas na Fila ${queue}, ${statusLabel(ipbxStatus)}, no período ${period}, concluída. Confira a planilha baixada pelo navegador.`;
    doneDialog.querySelector('#blockedDoneText').textContent=phrase;
    if(!doneDialog.open)doneDialog.showModal();
    try{
      sound=sound||new (window.AudioContext||window.webkitAudioContext)();
      sound.resume().then(()=>[0,.3].forEach((offset)=>{
        const tone=sound.createOscillator(),gain=sound.createGain();
        tone.frequency.value=740;gain.gain.setValueAtTime(.06,sound.currentTime+offset);
        gain.gain.exponentialRampToValueAtTime(.001,sound.currentTime+offset+.18);
        tone.connect(gain);gain.connect(sound.destination);tone.start(sound.currentTime+offset);tone.stop(sound.currentTime+offset+.2);
      })).catch(()=>{});
    }catch{}
  }
  function renderSummary(result){
    if(!summary)return;
    summary.replaceChildren();
    const carriers=Array.isArray(result?.carriers)?result.carriers:[];
    if(!carriers.length){summary.hidden=true;return;}
    summary.hidden=false;
    const heading=document.createElement('strong');
    heading.textContent=`Fila ${result.queue} · ${statusLabel(result.ipbx_status)} · ${result.rows} chamadas · operadoras`;
    summary.append(heading);
    for(const carrier of carriers){
      const row=document.createElement('div');row.className='blocked-carrier-row';
      const name=document.createElement('span');name.textContent=carrier.name;
      name.title=carrier.name;
      const value=document.createElement('b');
      value.textContent=`${Math.round(carrier.percent)}% · ${carrier.count}`;
      row.append(name,value);summary.append(row);
    }
  }
  fetch('/api/ipbx/blocked-calls/latest',{credentials:'same-origin',cache:'no-store'})
    .then(response=>response.ok?response.json():null)
    .then(result=>{
      if(!result||sessionStorage.getItem(storageKey))return;
      renderSummary(result);
    }).catch(()=>{});
  async function watchJob(job,queue,ipbxStatus='21'){
    for(;;){
      const statusResponse=await fetch(`/api/ipbx/blocked-calls/status?job=${encodeURIComponent(job)}`,{
        credentials:'same-origin',cache:'no-store'});
      if(!statusResponse.ok)throw new Error('Não foi possível acompanhar a consulta. Tente novamente.');
      const status=await statusResponse.json();
      ipbxStatus=String(status.ipbx_status||ipbxStatus);
      if(status.state==='failed')throw new Error(status.error||'Falha na consulta.');
      if(status.state==='done'){
        try{
          const response=await fetch(`/api/ipbx/blocked-calls/download?job=${encodeURIComponent(job)}`,{
            credentials:'same-origin',cache:'no-store'});
          if(response.ok){
            const url=URL.createObjectURL(await response.blob());
            const link=document.createElement('a');link.href=url;
            link.download=status.filename||`telefones_fila_${queue}_Rel${ipbxStatus}.xlsx`;
            document.body.append(link);link.click();link.remove();
            setTimeout(()=>URL.revokeObjectURL(url),60000);
          }
        }catch{}
        sessionStorage.removeItem(storageKey);
        message.textContent=`Fila ${queue} · ${statusLabel(ipbxStatus)}: ${status.rows} chamada${status.rows===1?'':'s'} processada${status.rows===1?'':'s'}. Planilha baixada pelo navegador.${status.portal_limit_possible?' O Portal retornou 1000 linhas; pode haver mais chamadas no período.':''}`;
        const result={queue,rows:status.rows,period:status.period,carriers:status.carriers,ipbx_status:ipbxStatus};
        renderSummary(result);
        notifyDone(queue,status.period,ipbxStatus);
        return;
      }
      message.textContent=`Fila ${queue} · ${statusLabel(ipbxStatus)}: ${status.stage||'Consulta em andamento'}…`;
      await wait(4000);
    }
  }
  buttons.forEach(button=>button.addEventListener('click',event=>{
    event.stopPropagation();
    selectedQueue=button.dataset.blockedQueue;
    if(!['0247400046','0247400047'].includes(selectedQueue))return;
    const yesterday=new Date();yesterday.setDate(yesterday.getDate()-1);
    const form=formDialog.querySelector('form');
    form.elements.startDate.value=dateText(yesterday);form.elements.endDate.value=dateText(yesterday);
    form.elements.startTime.value='00:00:00';form.elements.endTime.value='23:59:59';
    form.elements.ipbxStatus.value='21';
    formDialog.querySelector('#blockedCallsTitle').textContent=`Busca de chamadas · Fila ${selectedQueue}`;
    formDialog.querySelector('#blockedPeriodError').textContent='';formDialog.showModal();
  }));
  formDialog.querySelector('form').addEventListener('submit',async event=>{
    event.preventDefault();
    const form=event.target,queue=selectedQueue,ipbxStatus=form.elements.ipbxStatus.value;
    const start=validStamp(form.elements.startDate.value,form.elements.startTime.value);
    const end=validStamp(form.elements.endDate.value,form.elements.endTime.value);
    if(!start||!end||end.epoch<=start.epoch){
      formDialog.querySelector('#blockedPeriodError').textContent='Informe datas válidas; Data Fim deve ser posterior à Data Início.';
      return;
    }
    if(!Object.hasOwn(statusLabels,ipbxStatus)){
      formDialog.querySelector('#blockedPeriodError').textContent='Selecione um Status IPBX válido.';return;
    }
    formDialog.close();
    try{sound=sound||new (window.AudioContext||window.webkitAudioContext)();sound.resume().catch(()=>{});}catch{}
    buttons.forEach(button=>button.disabled=true);
    if(summary)summary.hidden=true;
    message.textContent=`Extraindo a fila ${queue} · ${statusLabel(ipbxStatus)} de ${start.value} a ${end.value}. Isso pode levar vários minutos…`;
    try{
      const meResponse=await fetch('/api/admin/me',{credentials:'same-origin',cache:'no-store'});
      if(!meResponse.ok)throw new Error('Entre novamente no TOI para gerar a planilha.');
      const me=await meResponse.json();
      const response=await fetch(`/api/ipbx/blocked-calls?queue=${queue}`,{
        method:'POST',credentials:'same-origin',headers:{'X-CSRF-Token':me.csrf,'Content-Type':'application/json'},
        body:JSON.stringify({start:start.value,end:end.value,ipbx_status:ipbxStatus})});
      if(!response.ok){
        const error=await response.json().catch(()=>({}));
        throw new Error(`${error.error||`Falha HTTP ${response.status}`}${error.reference?` · Ref. ${error.reference}`:''}`);
      }
      const started=await response.json();
      sessionStorage.setItem(storageKey,JSON.stringify({job:started.job,queue,ipbxStatus}));
      await watchJob(started.job,queue,ipbxStatus);
    }catch(error){sessionStorage.removeItem(storageKey);message.textContent=error.message||'Não foi possível gerar o relatório.';}
    finally{buttons.forEach(button=>button.disabled=false);}
  });
  const pending=sessionStorage.getItem(storageKey);
  if(pending){
    try{
      const {job,queue,ipbxStatus}=JSON.parse(pending);
      buttons.forEach(button=>button.disabled=true);
      watchJob(job,queue,ipbxStatus).catch(error=>{
        sessionStorage.removeItem(storageKey);
        message.textContent=error.message||'Não foi possível acompanhar a consulta.';
      }).finally(()=>buttons.forEach(button=>button.disabled=false));
    }catch{sessionStorage.removeItem(storageKey);}
  }
})();
