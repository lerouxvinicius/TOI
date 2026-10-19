let queues=[],currentPeriod="15m",refreshTimer=null,lastData=null,lastAnalytics=null,lastAnalyticsKey=null,lastAnalyticsLoadAt=0,analyticsInFlightKey=null,loadSeq=0,customRange=null,operationalContextConfig=null,systemHealthSnapshot=null,lastSystemHealthLoad=0,gatewayHealthSnapshot=null,lastGatewayHealthLoad=0,agirouteSnapshot=null,lastAgirouteLoad=0,analyticsCache=new Map();
let mlAudit=null;
let livePeriod=false,refreshSeconds=10;
let appInitialized=false,initializing=false,initRetryTimer=null;
let activeLoadKey='';const overviewInFlight=new Set();
const chartZoomState={key:null,start:0,end:1,minSpan:.01,dragging:false,lastX:null,lastEvents:[],profileMode:false};
const $=id=>document.getElementById(id);
const fmt=(v,d=2)=>v===null||v===undefined||v===""?"—":Number(v).toLocaleString("pt-BR",{minimumFractionDigits:d,maximumFractionDigits:d});
const pct=v=>v===null||v===undefined?"—":fmt(v)+"%"; const n0=v=>v===null||v===undefined?"—":fmt(v,0);
const PERIOD_LABELS={"5m":"5 min","15m":"15 min","30m":"30 min","1h":"60 min","2h":"2H","6h":"6H","1d":"24H","7d":"7 dias","14d":"14 dias","30d":"30 dias","custom":"Personalizado"};
function periodLabel(){return livePeriod?'Tempo real · 15 min':(PERIOD_LABELS[currentPeriod]||currentPeriod)}
function periodMinutes(){
  if(currentPeriod==="custom"&&customRange?.start&&customRange?.end){
    return Math.max(1,(new Date(customRange.end)-new Date(customRange.start))/60000);
  }
  return {"5m":5,"15m":15,"30m":30,"1h":60,"2h":120,"6h":360,"1d":1440,"7d":10080,"14d":20160,"30d":43200}[currentPeriod]||15
}
function periodUsesHistoricalPrimary(a=lastAnalytics){
  // Regra global H2-B: acima de 2h, a janela selecionada governa o card.
  // Nos botões fixos isso significa 6H/24H/7d/14d/30d; personalizados seguem a duração real.
  return Boolean(a)&&periodMinutes()>120;
}
function sourceCoveragePct(a,key){
  const c=a?.coverage?.[key]||{};
  return finiteNumber(c.data_coverage_pct??c.coverage_pct);
}
function sourceCoverageText(a,key,label){
  if(!a)return "";
  const v=sourceCoveragePct(a,key);
  return v===null?` · cobertura ${label} N/C`:` · cobertura ${label} ${fmt(v,2)}%`;
}
function congestionTone(v){
  if(v===null||v===undefined||v==="")return {cls:"neutral",color:"#8da0ad"};
  const n=Number(v);
  if(n<=10)return {cls:"green",color:"#65d27a"};
  if(n<=20)return {cls:"orange",color:"#ffb316"};
  return {cls:"red",color:"#ff3d45"};
}
function humanMinutes(v){
  if(v===null||v===undefined)return "—";
  const raw=Math.max(0,Number(v));
  if(raw>0&&raw<1)return `${Math.max(1,Math.round(raw*60))}s`;
  const m=Math.max(0,Math.round(raw));
  if(m<60)return `${m} min`;
  const h=Math.floor(m/60),rm=m%60;
  if(h<24)return rm?`${h}h ${rm}min`:`${h}h`;
  const d=Math.floor(h/24),rh=h%24;
  return rh?`${d}d ${rh}h`:`${d}d`;
}

function clean(v){const d=document.createElement("div");d.innerHTML=String(v??"");return (d.textContent||d.innerText||"").replace(/\s+/g," ").trim()}
function freshTag(x,label){
  const age=x?.age_s;
  const st=x?.status||"SEM_DADOS";
  const a=age===null||age===undefined?"—":`${Math.round(age)}s`;
  const icon=st==="LIVE"?"✓":st==="ATRASO"?"~":st==="STALE"?"!":st==="PAUSADO"?"⏸":"?";
  return `${label} ${a} ${icon}${st}`;
}
function formatUpdatedAt(d){
  const l=d?.latest||{};
  const raw=clean(l?.source_cache_time||l?.source_server_time||"");
  const found=raw.match(/\d{2}:\d{2}:\d{2}/g);
  const time=(found&&found.length?found[found.length-1]:String(l?.ts||"").slice(11,19))||"—";
  const s=d?.freshness?.sources||{};
  return `Atualizado ${time} · ${freshTag(s.queue,"Fila")} · ${freshTag(s.infra,"Infra")} · ${freshTag(s.stuck_channels,"IPBX")} · ${freshTag(s.dialer,"Disc")}`;
}
async function api(u,opts={}){
  const timeout=u.includes('/api/analytics/')?60000:15000;
  const r=await fetch(u,{cache:"no-store",signal:AbortSignal.timeout(timeout),...opts});
  if(r.status===401)window.dispatchEvent(new Event('toi-session-expired'));
  if(!r.ok)throw new Error(`HTTP ${r.status}: ${await r.text()}`);
  return r.json();
}

function setStateChip(id,prefix,label,tone){
  const e=$(id);
  if(!e)return;
  e.textContent=`${prefix} ${label}`;
  e.className=`state-chip ${tone}`;
}


async function loadGatewayHealthSnapshot(force=false){
  const now=Date.now();
  if(!force&&now-lastGatewayHealthLoad<8000)return gatewayHealthSnapshot;
  lastGatewayHealthLoad=now;
  try{
    const r=await fetch(`/toi_gateway_health_latest.json?_=${now}`,{cache:"no-store"});
    if(!r.ok)throw new Error(`HTTP ${r.status}`);
    gatewayHealthSnapshot=await r.json();
  }catch(e){gatewayHealthSnapshot=null}
  return gatewayHealthSnapshot;
}
function normEntityName(v){return String(v||"").toUpperCase().replace(/[^A-Z0-9]/g,"")}
function displayGatewayName(v){return String(v||"").trim().toUpperCase()}
function gatewayTechnicalStatusLabel(g){
  if(!g||g.live_available===false)return "N/C";
  const st=String(g.status||g.machine_status||g.stable_status||"").toUpperCase();
  return ({OPERACIONAL:"Operacional",REACHABLE:"Alcançável",PING_DOWN:"ICMP indisponível",PING_INTERMITTENT:"ICMP intermitente",PING_RECOVERING:"ICMP recuperando",PATH_DEGRADED:"Path degradado",OPERACIONAL_ICMP_NC:"Operacional · ICMP N/C",OPERACIONAL_ICMP_INTERMITTENT:"Operacional · ICMP instável",SERVICE_ACTIVE_ICMP_DOWN:"Serviço ativo · ICMP DOWN",NO_TRAFFIC_WARN:"Sem tráfego",NO_TRAFFIC_OBSERVE:"Sem tráfego · observação",NO_TRAFFIC_CRITICAL:"Sem tráfego crítico",MISSING_API:"Sem dado TOI",CONNECTIVITY_PATH:"VPN/Rede",LEARNING:"Aprendendo",OUTSIDE_WINDOW:"Alcançável",SEM_IP_CADASTRADO:"Sem IP cadastrado",PING_NC:"ICMP N/C"})[st]||st||"N/C";
}
function gatewayTechnicalTone(g){
  if(!g||g.live_available===false)return "neutral";
  const s=String(g.status||g.machine_status||g.stable_status||"").toUpperCase();
  if(["PING_DOWN","NO_TRAFFIC_CRITICAL"].includes(s))return "bad";
  if(["PING_INTERMITTENT","NO_TRAFFIC_WARN","PING_NC","OPERACIONAL_ICMP_NC","OPERACIONAL_ICMP_INTERMITTENT","SERVICE_ACTIVE_ICMP_DOWN","PATH_DEGRADED","PING_RECOVERING","MISSING_API","SEM_IP_CADASTRADO"].includes(s))return "warn";
  if(["OPERACIONAL","REACHABLE"].includes(s))return "ok";
  if(["CONNECTIVITY_PATH","LEARNING","OUTSIDE_WINDOW","NO_TRAFFIC_OBSERVE"].includes(s))return "context";
  return "neutral";
}
function gatewayIcmpSummary(g){
  if(!g||g.live_available===false)return {label:"N/C",tone:"neutral"};
  const reach=String(g.reachability_state||g.reachability||"").toUpperCase();
  const status=String(g.status||g.machine_status||g.stable_status||"").toUpperCase();
  const loss=finiteNumber(g.loss_pct);
  if(reach==="DOWN"||(loss!==null&&loss>=100)||["PING_DOWN","SERVICE_ACTIVE_ICMP_DOWN"].includes(status)){
    return {label:"Sem Resposta",tone:"bad"};
  }
  if(reach==="INTERMITTENT"||["PING_INTERMITTENT","OPERACIONAL_ICMP_INTERMITTENT"].includes(status)){
    return {label:"Intermitente",tone:"warn"};
  }
  if(reach==="PATH_UNCERTAIN"||["PATH_DEGRADED","PING_RECOVERING"].includes(status)){
    return {label:"Inconclusivo",tone:"warn"};
  }
  if(reach==="OK"||["OPERACIONAL","REACHABLE","OUTSIDE_WINDOW"].includes(status)){
    return {label:"Ping OK",tone:"ok"};
  }
  return {label:"N/C",tone:"neutral"};
}
function gatewayContextLabel(g){
  const sem=gatewaySemanticCode(g);
  return ({FAULT_CONFIRMED_CONTEXT:"Falha confirmada",RECOVERED_CONFIRMED:"Recuperado confirmado",PLANNED_INACTIVE:"Parada planejada",STANDBY_EXPECTED:"Standby esperado",OPERATIONAL_PARTIAL_VISIBILITY:"Visibilidade parcial"})[sem]||"Sem contexto especial";
}
function gatewayContextTone(g){
  const sem=gatewaySemanticCode(g);
  if(sem==="FAULT_CONFIRMED_CONTEXT")return "bad";
  if(sem==="RECOVERED_CONFIRMED")return "ok";
  if(sem==="OPERATIONAL_PARTIAL_VISIBILITY")return "warn";
  if(["PLANNED_INACTIVE","STANDBY_EXPECTED"].includes(sem))return "context";
  return "neutral";
}
function trunkPressureClass(v){const n=finiteNumber(v);return n===null?"nc":n>=90?"high":n>=80?"mid":"ok"}
function gatewayHealthInfo(){
  const s=gatewayHealthSnapshot;
  if(!s)return {usable:false,age_s:null,instance:null,alerts:[],gateways:[],connectivity:{}};
  let age=null;const t=new Date(s.generated_at||"");
  if(!Number.isNaN(t.getTime()))age=Math.max(0,(Date.now()-t.getTime())/1000);
  return {usable:age!==null&&age<=45,age_s:age,instance:String($("instanceSelect")?.value||""),
    alerts:Array.isArray(s.alerts)?s.alerts:[],gateways:Array.isArray(s.gateways)?s.gateways:[],
    connectivity:s.connectivity||{},raw:s};
}
function gatewaySemanticRows(){
  const src=lastData?.gateway_semantic_context;
  return Array.isArray(src?.gateways)?src.gateways:[];
}
function gatewaySemanticFor(entity){
  const target=normEntityName(entity);
  return gatewaySemanticRows().find(x=>normEntityName(x.entity)===target)||null;
}
function healthForGateway(entity){
  const info=gatewayHealthInfo(),target=normEntityName(entity),sem=gatewaySemanticFor(entity);
  let live=null;
  if(info.usable){
    live=info.gateways.find(g=>{
      if(String(g.instancia)!==info.instance)return false;
      return [g.entity,g.config_name,...(g.aliases||[])].map(normEntityName).includes(target);
    })||null;
  }
  if(!live&&!sem)return null;
  return {...(sem||{}),...(live||{}),semantic_context:sem||null,live_available:!!live};
}
function gatewaySemanticCode(g){
  const sem=g?.semantic_context||g||{};
  return String(sem.canonical_status||g?.canonical_status||g?.canonical_semantic_status||g?.semantic_status||g?.semantic||"").toUpperCase();
}
function gatewayTone(status,g=null){
  const sem=gatewaySemanticCode(g);
  if(sem==="FAULT_CONFIRMED_CONTEXT")return "bad";
  if(sem==="RECOVERED_CONFIRMED")return "ok";
  if(sem==="PLANNED_INACTIVE")return "context";
  if(sem==="STANDBY_EXPECTED")return "context";
  if(sem==="OPERATIONAL_PARTIAL_VISIBILITY")return "warn";
  const s=String(status||"").toUpperCase();
  if(["PING_DOWN","NO_TRAFFIC_CRITICAL"].includes(s))return "bad";
  if(["PING_INTERMITTENT","NO_TRAFFIC_WARN","PING_NC","OPERACIONAL_ICMP_NC",
      "OPERACIONAL_ICMP_INTERMITTENT","SERVICE_ACTIVE_ICMP_DOWN","PATH_DEGRADED",
      "PING_RECOVERING","MISSING_API","SEM_IP_CADASTRADO"].includes(s))return "warn";
  if(["OPERACIONAL","REACHABLE"].includes(s))return "ok";
  if(["CONNECTIVITY_PATH","LEARNING","OUTSIDE_WINDOW","NO_TRAFFIC_OBSERVE"].includes(s))return "context";
  return "neutral";
}
function gatewayStatusLabel(g){
  if(!g)return "N/C";
  const sem=gatewaySemanticCode(g);
  const semLabel={
    FAULT_CONFIRMED_CONTEXT:"Falha confirmada",
    RECOVERED_CONFIRMED:"Recuperado confirmado",
    PLANNED_INACTIVE:"Parada planejada",
    STANDBY_EXPECTED:"Standby esperado",
    OPERATIONAL_PARTIAL_VISIBILITY:"Visibilidade parcial"
  }[sem];
  if(semLabel)return semLabel;
  return ({
    OPERACIONAL:"Operacional",
    REACHABLE:"Alcançável",
    PING_DOWN:"ICMP indisponível",
    PING_INTERMITTENT:"ICMP intermitente",
    PING_RECOVERING:"ICMP recuperando",
    PATH_DEGRADED:"ICMP inconclusivo",
    OPERACIONAL_ICMP_NC:"Operacional · ICMP N/C",
    OPERACIONAL_ICMP_INTERMITTENT:"Operacional · ICMP instável",
    SERVICE_ACTIVE_ICMP_DOWN:"Serviço ativo · ICMP DOWN",
    NO_TRAFFIC_WARN:"Sem tráfego",
    NO_TRAFFIC_OBSERVE:"Sem tráfego · observação",
    NO_TRAFFIC_CRITICAL:"Sem tráfego crítico",
    MISSING_API:"Sem dado TOI",
    CONNECTIVITY_PATH:"VPN/Rede",
    LEARNING:"Aprendendo",
    OUTSIDE_WINDOW:"Alcançável",
    SEM_IP_CADASTRADO:"Sem IP cadastrado",
    PING_NC:"ICMP N/C"
  })[String(g.status||g.machine_status||"").toUpperCase()]||String(g.status||g.machine_status||"N/C");
}
function gatewayReachabilityLabel(g){
  if(!g)return "N/C";
  if(g.live_available===false)return "Live N/C";
  const r=String(g.reachability_state||g.reachability||"").toUpperCase();
  const base=({OK:"OK",INTERMITTENT:"Intermitente",DOWN:"DOWN",NC:"N/C",PATH_UNCERTAIN:"Inconclusivo"})[r]||r||"N/C";
  const loss=finiteNumber(g.loss_pct);
  return loss===null?base:`${base} · perda ${loss.toFixed(2)}%`;
}
function gatewayBaselineLabel(g){
  if(!g)return "baseline N/C";
  const cls=String(g.baseline_class||"").toUpperCase();
  const label=({ACTIVE_RECURRENT:"recorrente",ACTIVE_OCCASIONAL:"ocasional",STANDBY_CANDIDATE:"standby candidato",LEARNING:"aprendendo"})[cls]||"N/C";
  const ratio=finiteNumber(g.baseline_active_ratio);
  return ratio===null?`baseline ${label}`:`baseline ${label} · ativo ${ratio.toFixed(2)}%`;
}
function gatewayRoleLabel(g){
  const sem=g?.semantic_context||g||{};
  const role=String(sem.operational_role||"").toUpperCase();
  const traffic=String(sem.traffic_expectation||"").toUpperCase();
  const roleLabel={
    SBC_LAST_ROUTE:"SBC · última rota",
    RECEPTIVE_GATEWAY:"Gateway receptivo",
    FUTURE_SBC:"Futuro SBC",
    STANDARD_GATEWAY_ROLE_UNSPECIFIED:"Gateway padrão",
    BALANCED_GATEWAY:"Gateway com tráfego balanceado",STANDBY_GATEWAY:"Gateway standby"
  }[role]||"Papel N/C";
  const trafficLabel={
    SPORADIC_LAST_ROUTE:"tráfego esporádico esperado",
    RECEPTIVE_LOW_OR_VARIABLE:"tráfego receptivo variável",
    NO_TRAFFIC_PLANNED:"sem tráfego planejado",
    TRAFFIC_BLOCKED_BY_CONFIRMED_FAULT:"tráfego bloqueado por falha",
    TRAFFIC_RESTORED:"tráfego restabelecido",
    BALANCED_TRAFFIC:"participação esperada no balanceamento"
  }[traffic];
  return trafficLabel?`${roleLabel} · ${trafficLabel}`:roleLabel;
}
function gatewayCapacityLabel(g){
  const sem=g?.semantic_context||g||{};
  if(sem.capacity_label)return sem.capacity_label;
  return ({
    FAULT_CONFIRMED_CONTEXT:"Reduz resiliência; não contar como redundância saudável",
    RECOVERED_CONFIRMED:"Recuperado; observar telemetria atual",
    PLANNED_INACTIVE:"Não conta como capacidade imediata",
    STANDBY_EXPECTED:"Standby; contingência potencial",
    OPERATIONAL_PARTIAL_VISIBILITY:"Não penaliza capacidade; reduz confiança"
  })[gatewaySemanticCode(g)]||"Observacional";
}
function gatewayTrafficLabel(g){
  if(!g)return "N/C";
  const sem=g?.semantic_context||g||{};
  if(sem.operational_role||sem.traffic_expectation)return gatewayRoleLabel(g);
  const share=finiteNumber(g.traffic_share_pct),call=finiteNumber(g.call_share_pct);
  const a=share===null?"share N/C":`share ${share.toFixed(2)}%`;
  const b=call===null?"calls N/C":`calls ${call.toFixed(2)}%`;
  return `${a} · ${b} · ${gatewayBaselineLabel(g)}`;
}
function gatewayTooltip(g,status){
  if(!g)return status||"N/C";
  const sem=g?.semantic_context||g||{};
  const parts=[status];
  if(sem.note)parts.push(sem.note);
  if(sem.evidence_source)parts.push(`evidência ${sem.evidence_source}`);
  if(g.detail||g.reason)parts.push(g.detail||g.reason);
  if(g.ip)parts.push(`IP ${g.ip}`);
  if(g.rtt_p50_ms!==null&&g.rtt_p50_ms!==undefined)parts.push(`RTT p50 ${Math.round(g.rtt_p50_ms)}ms`);
  if(g.rtt_p95_ms!==null&&g.rtt_p95_ms!==undefined)parts.push(`95% das respostas até ${Math.round(g.rtt_p95_ms)}ms`);
  if(g.rtt_jitter_ms!==null&&g.rtt_jitter_ms!==undefined)parts.push(`jitter ${Math.round(g.rtt_jitter_ms)}ms`);
  parts.push(gatewayTrafficLabel(g));
  parts.push(gatewayCapacityLabel(g));
  const h=g.hysteresis||{};
  if(h.candidate)parts.push(`histerese ${h.candidate} ${h.count||0}/${h.required||0}`);
  if(g.path_suppressed)parts.push("falha individual suprimida por Path Quorum");
  return parts.filter(Boolean).join(" | ");
}

function semanticContextsForCurrentInstance(){
  const inst=String($("instanceSelect")?.value||"");
  return gatewaySemanticRows().filter(x=>String(x.instancia)===inst);
}
function semanticAlertRows(){
  return semanticContextsForCurrentInstance().map(x=>{
    const code=String(x.canonical_status||"").toUpperCase();
    if(code==="FAULT_CONFIRMED_CONTEXT")return {cls:"bad",tag:"GT",text:`${x.entity}: falha confirmada; reduz resiliência até revalidação.`,title:x.note||""};
    if(code==="PLANNED_INACTIVE")return {cls:"context",tag:"PLAN",text:`${x.entity}: parada planejada/fornecedor; fora da capacidade imediata.`,title:x.note||""};
    if(code==="STANDBY_EXPECTED")return {cls:"context",tag:"STBY",text:`${x.entity}: SBC/standby esperado; baixa ocupação pode ser normal.`,title:x.note||""};
    if(code==="OPERATIONAL_PARTIAL_VISIBILITY")return {cls:"warn",tag:"VIS",text:`${x.entity}: visibilidade parcial; reduz confiança sem indicar falha.`,title:x.note||""};
    return null;
  }).filter(Boolean);
}
function renderGatewayExecutiveChip(){
  const info=gatewayHealthInfo(),ctx=semanticContextsForCurrentInstance();
  const faults=ctx.filter(x=>x.canonical_status==="FAULT_CONFIRMED_CONTEXT");
  const partial=ctx.filter(x=>x.canonical_status==="OPERATIONAL_PARTIAL_VISIBILITY");
  const planned=ctx.filter(x=>x.canonical_status==="PLANNED_INACTIVE");
  const standby=ctx.filter(x=>x.canonical_status==="STANDBY_EXPECTED");
  let label="N/C",tone="neutral",title="Gateway Health Shadow indisponível";
  if(info.usable){
    const inst=info.instance,alerts=info.alerts.filter(a=>String(a.instancia)===inst);
    const crit=alerts.filter(a=>String(a.severity).toUpperCase()==="CRITICAL");
    const warn=alerts.filter(a=>String(a.severity).toUpperCase()==="WARN");
    const path=String(info.connectivity?.status||"").toUpperCase();
    if(["REDE","CONNECTIVITY_PATH_UNAVAILABLE"].includes(path)){
      label="REDE";tone="context";title="Conectividade corporativa/VPN indisponível ou sem quorum; críticos individuais ICMP são suprimidos.";
    }else if(path==="DEGRADED"){
      label="REDE DEG";tone="warn";title="Quorum ICMP degradado; falhas individuais inconclusivas são suprimidas.";
    }else if(path==="RECOVERY"){
      label="RECUP";tone="context";title="Caminho recuperou; Gateway Health está em grace period antes de voltar a julgar falhas individuais.";
    }else if(crit.length){label=`CRÍTICO ${crit.length}`;tone="bad";title=crit.map(a=>a.message).join(" | ")}
    else if(faults.length){label="RESIL ↓";tone="warn";title=`Ground Truth: ${faults.map(x=>x.entity).join(", ")} com falha confirmada até revalidação.`}
    else if(warn.length){label=`ATENÇÃO ${warn.length}`;tone="warn";title=warn.map(a=>a.message).join(" | ")}
    else if(partial.length){label="VIS PARC";tone="warn";title=`Visibilidade parcial: ${partial.map(x=>x.entity).join(", ")}.`}
    else{
      const gs=info.gateways.filter(g=>String(g.instancia)===inst&&g.role==="GW");
      label=gs.length?`OK ${gs.length}`:(ctx.length?`CTX ${ctx.length}`:"N/C");tone=gs.length?"ok":ctx.length?"context":"neutral";
      title=gs.length?`${gs.length} gateways em Gateway Health Shadow.${standby.length?` ${standby.length} standby esperado.`:""}${planned.length?` ${planned.length} parada planejada.`:""}`:"Contexto sem telemetria LIVE recente.";
    }
  }else if(ctx.length){
    label=faults.length?"RESIL ↓":partial.length?"VIS PARC":`CTX ${ctx.length}`;
    tone=faults.length||partial.length?"warn":"context";
    title=`Telemetria Gateway Health sem snapshot recente; Ground Truth operacional preservado para ${ctx.length} gateway(s).`;
  }
  setStateChip("gatewayStatusChip","GW",label,tone);
  const e=$("gatewayStatusChip");if(e)e.title=title;
}
function renderInfrastructureAlerts(){
  const info=gatewayHealthInfo(),inst=String($("instanceSelect")?.value||""),el=$("factors");
  if(!el)return;
  const rows=[];
  if(info.usable){
    const path=String(info.connectivity?.status||"").toUpperCase();
    if(["REDE","CONNECTIVITY_PATH_UNAVAILABLE"].includes(path))rows.push({cls:"context",tag:"REDE",text:"Quorum de conectividade indisponível; falhas individuais de ICMP suprimidas."});
    else if(path==="DEGRADED")rows.push({cls:"warn",tag:"REDE",text:"Monitoramento ICMP degradado em múltiplos Gateways; ainda não tratado como falha individual."});
    else if(path==="RECOVERY")rows.push({cls:"context",tag:"REC",text:"Conectividade recuperada; grace period em andamento antes de reativar críticos individuais."});
    (info.alerts||[]).filter(a=>String(a.instancia)===inst).slice(0,3).forEach(a=>{
      const sev=String(a.severity||"WARN").toUpperCase();
      rows.push({cls:sev==="CRITICAL"?"bad":"warn",tag:sev==="CRITICAL"?"CRIT":"ATN",text:a.message||a.type||"Alerta de Gateway",title:a.message||""});
    });
  }
  semanticAlertRows().forEach(x=>rows.push(x));
  if(!rows.length){
    if(info.usable){
      const n=info.gateways.filter(g=>String(g.instancia)===inst&&g.role==="GW").length;
      rows.push({cls:"ok",tag:"OK",text:`${n} Gateway${n===1?"":"s"} monitorado${n===1?"":"s"}; nenhuma restrição crítica conhecida.`});
    }else rows.push({cls:"neutral",tag:"N/C",text:"Gateway Health Shadow sem snapshot recente e sem contexto operacional especial para esta Instância."});
  }
  el.innerHTML=rows.slice(0,4).map(r=>`<div class="infra-alert ${r.cls}" title="${String(r.title||"").replace(/"/g,"&quot;")}"><b>${r.tag}</b><span>${r.text}</span></div>`).join("");
}


async function loadSystemHealthSnapshot(force=false){
  const now=Date.now();
  if(!force&&now-lastSystemHealthLoad<15000)return systemHealthSnapshot;
  lastSystemHealthLoad=now;
  try{
    const r=await fetch(`/toi_system_health_latest.json?_=${now}`,{cache:"no-store"});
    if(!r.ok)throw new Error(`HTTP ${r.status}`);
    systemHealthSnapshot=await r.json();
  }catch(e){
    systemHealthSnapshot=null;
  }
  return systemHealthSnapshot;
}
async function loadAgirouteSnapshot(force=false){
  const now=Date.now();
  if(!force&&now-lastAgirouteLoad<15000)return agirouteSnapshot;
  lastAgirouteLoad=now;
  try{
    const response=await fetch(`/toi_agiroute_latest.json?_=${now}`,{cache:"no-store"});
    if(!response.ok)throw new Error(`HTTP ${response.status}`);
    agirouteSnapshot=await response.json();
  }catch(_error){agirouteSnapshot=null}
  renderAgiroutePanel();
  return agirouteSnapshot;
}
function renderAgiroutePanel(){
  const panel=$("agiroutePanel"),status=$("agirouteStatus");
  if(!panel||!status)return;
  panel.hidden=String($("instanceSelect")?.value||"")!=="6";
  if(panel.hidden)return;
  const doc=agirouteSnapshot,stamp=Date.parse(doc?.generated_at||"");
  const age=Number.isFinite(stamp)?Math.max(0,(Date.now()-stamp)/1000):Infinity;
  if(!doc||doc.schema!=="toi_agiroute_health_v1"||age>150){
    status.textContent="N/C · verificação dos servidores ausente ou atrasada.";
    panel.classList.add("agiroute-warning");return;
  }
  if(!doc.configured){
    status.textContent="N/C · acesso SSH de leitura ainda não configurado.";
    panel.classList.add("agiroute-warning");return;
  }
  const hosts=Array.isArray(doc.hosts)?doc.hosts:[];
  const lines=hosts.map(h=>{
    if(h.status!=="OK")return `${h.ip}: ${h.status==="UNREACHABLE"?"acesso SSH indisponível":"verificação sem resposta válida"}`;
    const units=["agiroutey","proxysql"].map(name=>{
      const unit=h.services?.[name];
      return `${name} ${unit?.status==="OK"?"ativo":unit?.status==="DOWN"?"PARADO":"N/C"}${["disabled","masked"].includes(unit?.boot)?" (sem início automático)":""}`;
    });
    return `${h.ip}: ${units.join(" · ")}`;
  });
  const good=hosts.length===2&&hosts.every(h=>h.status==="OK"&&["agiroutey","proxysql"].every(name=>h.services?.[name]?.status==="OK"&&!["disabled","masked"].includes(h.services?.[name]?.boot)));
  panel.classList.toggle("agiroute-warning",!good);
  status.textContent=lines.join(" | ")+` · verificado há ${humanAge(age)}`;
  status.title=lines.join("\n");
}
function systemHealthInfo(){
  const s=systemHealthSnapshot;
  if(!s)return {usable:false,reason:"snapshot ausente",age_s:null,current:null,historical:null,cause:null};
  const raw=s.generated_at;
  let age=null;
  if(raw){
    const t=new Date(raw);
    if(!Number.isNaN(t.getTime()))age=Math.max(0,(Date.now()-t.getTime())/1000);
  }
  const usable=age!==null&&age<=300;
  const current=String(s?.current_health?.status||"").toUpperCase();
  const historical=String(s?.historical_reliability?.status||"").toUpperCase();
  const interruptions=s?.transversal_interruptions||[];
  const cause=interruptions.length?String(interruptions[interruptions.length-1]?.cause_class||""):"";
  return {usable,age_s:age,current,historical,cause,raw:s};
}
function renderSystemHealthExecutive(){
  const info=systemHealthInfo();
  const e=$("systemHealthExecutive");
  if(!e)return;
  if(!info.usable){
    const age=info.age_s===null?"":` · System Health ${humanAge(info.age_s)}`;
    e.textContent=`TOI API OK${age}`;
    e.className="system-health-exec ok";
    e.title="O Dashboard/API respondeu nesta atualização; o snapshot aprofundado de System Health está ausente ou antigo.";
    return;
  }
  const current=info.current||"N/C";
  const hist=info.historical||"—";
  let cause="";
  if(info.cause==="PLATFORM_OR_HOST_SILENCE")cause=" · último evento: host/plataforma";
  else if(info.cause==="EXTERNAL_SOURCE_OR_PIPELINE")cause=" · último evento: conectividade/fonte externa";
  else if(info.cause)cause=` · último evento: ${info.cause}`;
  e.textContent=`TOI ${current} · histórico 6h ${hist}${cause}`;
  e.className=`system-health-exec ${current==="SAUDÁVEL"?"ok":current==="DEGRADADO"?"warn":"neutral"}`;
}
function humanAge(sec){
  if(sec===null||sec===undefined)return "—";
  const s=Math.max(0,Math.round(sec));
  if(s<60)return `${s}s`;
  if(s<3600)return `${Math.floor(s/60)}m`;
  return `${Math.floor(s/3600)}h`;
}
function coverageGateFor(d){
  const cov=d?.period_coverage||{};
  const pct=Math.max(0,Math.min(100,finiteNumber(cov.coverage_pct)??(cov.complete?100:0)));
  let code,label,tone;
  if(cov.complete||pct>=95){code="COMPLETE";label="COMPLETA";tone="ok"}
  else if(pct>=70){code="PARTIAL";label="PARCIAL";tone="warn"}
  else if(pct>=30){code="FORMING";label="FORMANDO";tone="context"}
  else{code="INSUFFICIENT";label="INSUFICIENTE";tone="bad"}
  return {pct,code,label,tone,complete:!!cov.complete,samples:cov.samples||0,available_minutes:cov.available_minutes};
}
function renderCoverageGate(d,opctx){
  const cg=coverageGateFor(d);
  $("coverageGateValue").textContent=`${fmt(cg.pct,2)}%`;
  $("coverageGateBadge").textContent=cg.label;
  $("coverageGateBadge").className=`coverage-gate-badge ${cg.tone}`;
  $("coverageGateBar").style.width=`${cg.pct}%`;
  $("coverageGateBar").className=cg.tone;
  const mode=$("diagnosticModeBadge");
  if(opctx?.afterHours){
    mode.textContent="OBSERVAÇÃO";
    mode.className="diagnostic-mode-badge context";
  }else if(cg.pct<70){
    mode.textContent="PRELIMINAR";
    mode.className="diagnostic-mode-badge warn";
  }else{
    mode.textContent="CONSOLIDADO";
    mode.className="diagnostic-mode-badge ok";
  }
  return cg;
}


async function loadOperationalContextConfig(){
  try{
    operationalContextConfig=await api("/toi_operational_context_v1.json");
  }catch(e){
    operationalContextConfig=null;
    console.warn("Operational Context config indisponível; contexto horário não será aplicado.",e);
  }
}
function timeMinutes(hhmm){
  const m=String(hhmm||"").match(/^(\d{1,2}):(\d{2})$/);
  if(!m)return null;
  return Math.max(0,Math.min(1439,(+m[1])*60+(+m[2])));
}
function operationalContextFor(d){
  const l=d?.latest||{};
  const inst=String($("instanceSelect")?.value||l.instancia||"");
  const code=String($("queueSelect")?.value||l.codigo||"");
  const qkey=`${inst}:${code}`;
  const cfg=operationalContextConfig||{};
  const base=cfg.default||{};
  const specific=(cfg.queues||{})[qkey]||(cfg.queues||{})[code]||{};
  const c={...base,...specific};
  if(c.enabled===false)return {code:"NAO_CONFIGURADO",label:"CONTEXTO OFF",afterHours:false,configured:false};

  const start=timeMinutes(c.start_time);
  const end=timeMinutes(c.end_time);
  if(start===null||end===null){
    return {code:"NAO_CONFIGURADO",label:"JANELA N/C",afterHours:false,configured:false,startTime:c.start_time,endTime:c.end_time};
  }

  const ref=new Date();
  const nowMin=ref.getHours()*60+ref.getMinutes();

  let inside=false;
  if(start<end) inside=nowMin>=start&&nowMin<end;
  else if(start>end) inside=nowMin>=start||nowMin<end;
  else inside=true;

  const windowText=`${c.start_time}–${c.end_time}`;

  if(!inside){
    const beforeStart=start<end&&nowMin<start;
    const reason=beforeStart?`antes de ${c.start_time}`:`após ${c.end_time}`;
    return {
      code:"FORA_JANELA",
      label:`FORA DA JANELA · ${windowText}`,
      afterHours:true,
      configured:true,
      startTime:c.start_time,
      endTime:c.end_time,
      windowText,
      reason,
      contextText:`fora da janela operacional ${windowText}`
    };
  }

  return {
    code:"JANELA_OPERACIONAL",
    label:`JANELA OPERACIONAL · ${windowText}`,
    afterHours:false,
    configured:true,
    startTime:c.start_time,
    endTime:c.end_time,
    windowText,
    reason:"dentro da janela",
    contextText:`dentro da janela operacional ${windowText}`
  };
}
function sourceTone(status){
  const s=String(status||"SEM_DADOS").toUpperCase();
  if(s==="LIVE")return "ok";
  if(s==="ATRASO")return "warn";
  if(s==="PAUSADO")return "paused";
  if(s==="STALE"||s==="SEM_DADOS")return "bad";
  return "neutral";
}
function sourceBadge(label,info){
  const st=info?.status||"SEM_DADOS";
  const age=info?.age_s;
  const ageTxt=age===null||age===undefined?"—":`${Math.round(age)}s`;
  const target=finiteNumber(info?.target_s);
  const cadence=label==="FILA"&&target!==null?` title="Cadência planejada: ${Math.round(target)} segundos"`:"";
  return `<span class="source-badge ${sourceTone(st)}"${cadence}><b>${label}</b><i>${ageTxt}</i><em>${st}</em></span>`;
}
function renderFreshnessHeader(d){
  const s=d?.freshness?.sources||{};
  const raw=clean(d?.latest?.source_cache_time||d?.latest?.source_server_time||"");
  const found=raw.match(/\d{2}:\d{2}:\d{2}/g);
  const time=(found&&found.length?found[found.length-1]:String(d?.latest?.ts||"").slice(11,19))||"—";
  const queueTarget=finiteNumber(s.queue?.target_s);
  $("updatedAt").textContent=`Atualizado ${time}${queueTarget!==null&&queueTarget>=60?` · coleta da fila a cada ${Math.round(queueTarget/60)} min`:""}`;
  $("sourceBadges").innerHTML=[
    sourceBadge("FILA",s.queue),
    sourceBadge("INFRA",s.infra),
    sourceBadge("IPBX",s.stuck_channels),
    sourceBadge("DISC",s.dialer),
  ].join("");
  const q=finiteNumber(d?.quality_gate?.score);
  const qb=$("qualitySummaryBadge");
  const overall=String(d?.freshness?.overall||"").toUpperCase();
  qb.textContent=`QUALIDADE ${q===null?"—":fmt(q,2)+"%"}`;
  qb.className=`quality-summary-badge ${overall==="LIVE"?"ok":overall==="PARCIAL"?"warn":overall==="STALE"?"bad":"neutral"}`;
}
function contextualFactor(x,d,ctx){
  const text=String(x?.text||"");
  const after=ctx?.afterHours;
  const contextInvalid=after&&(text==="Taxa de discagem elevada"||text==="Nenhum agente disponível");
  if(contextInvalid){
    return `<div class="factor context-muted" title="Peso bruto do motor: ${x.impact}. Contextualizado na UI porque a operação está ${ctx.contextText||"fora da janela operacional"}."><b>CTX</b>${text} · esperado fora da janela</div>`;
  }
  if(text==="Fonte/instância com degradação de latência"){
    const sh=d?.source_health||{};
    const detail=`média ${fmt(sh.avg_latency,2)}s · máx ${fmt(sh.max_latency,2)}s · erros ${n0(sh.errors_window)}`;
    return `<div class="factor"><b class="neg">${x.impact}</b>${text} · ${detail}</div>`;
  }
  if(x?.kind==="quality")return `<div class="factor quality"><b>!</b>${text}</div>`;
  return `<div class="factor"><b class="${x.impact<0?"neg":"pos"}">${x.impact>0?"+":""}${x.impact}</b>${text}</div>`;
}
function renderStateTriad(d,crit,ctx){
  const s=d?.freshness?.sources||{},g=d?.diagnosis||{},sh=d?.source_health||{};
  const queueUsable=s?.queue?.usable===true||["LIVE","ATRASO"].includes(s?.queue?.status);
  let opLabel,opTone;
  if(ctx?.afterHours){opLabel="FORA JANELA";opTone="context"}
  else if(!queueUsable){opLabel="INDETERMINADA";opTone="warn"}
  else if(crit){opLabel="INCIDENTE";opTone="bad"}
  else if(g?.severity==="ATENÇÃO"){opLabel="ATENÇÃO";opTone="warn"}
  else{opLabel="NORMAL";opTone="ok"}
  const overall=String(d?.freshness?.overall||"").toUpperCase();
  const dataLabel=overall==="LIVE"?"ÍNTEGROS":overall==="PARCIAL"?"PARCIAIS":overall||"SEM DADOS";
  const dataTone=overall==="LIVE"?"ok":overall==="PARCIAL"?"warn":"bad";
  const shi=systemHealthInfo();
  const apiPlatform=String(d?.platform_health?.status||"").toUpperCase();
  // Se a própria API acabou de entregar este overview, a plataforma não é N/C.
  // Snapshot aprofundado stale continua indicado no tooltip, sem sobrepor a verdade corrente.
  const platform=shi.usable?shi.current:(apiPlatform||"API_OK");
  const platLabel=platform==="SAUDÁVEL"?"SAUDÁVEL":platform==="API_OK"?"API OK":platform==="DEGRADADO"||platform==="DEGRADADA"?"DEGRADADO":platform==="FALHA"?"FALHA":"N/C";
  const platTone=platform==="SAUDÁVEL"||platform==="API_OK"?"ok":platform==="DEGRADADO"||platform==="DEGRADADA"?"warn":platform==="FALHA"?"bad":"neutral";
  setStateChip("operationStatusChip","OP",opLabel,opTone);
  setStateChip("dataStatusChip","DADOS",dataLabel,dataTone);
  setStateChip("platformStatusChip","TOI",platLabel,platTone);
  const platformChip=$("platformStatusChip");
  if(platformChip){
    if(shi.usable){
      const hist=shi.historical||"N/C";
      let cause="";
      if(shi.cause==="PLATFORM_OR_HOST_SILENCE")cause=" · último evento: host/plataforma";
      else if(shi.cause==="EXTERNAL_SOURCE_OR_PIPELINE")cause=" · último evento: conectividade/fonte externa";
      else if(shi.cause)cause=` · último evento: ${shi.cause}`;
      platformChip.title=`System Health atual ${shi.current||platLabel} · histórico 6h ${hist}${cause}`;
    }else{
      const shMsg=shi.age_s===null?"System Health snapshot ausente":`System Health snapshot antigo · ${humanAge(shi.age_s)}`;
      platformChip.title=`API do TOI respondeu nesta atualização · ${shMsg}`;
    }
  }
}


function fitStage(){
  const wrap=$("fitWrap"),stage=$("goldenStage");
  if(wrap){wrap.removeAttribute("style");}
  if(stage){stage.removeAttribute("style");}
  const kpis=document.querySelector('.kpi-row');
  if(kpis){
    if(matchMedia('(max-width:700px)').matches){
      kpis.style.setProperty('top','0','important');
      kpis.style.setProperty('width','100%','important');
      kpis.style.setProperty('height','auto','important');
      kpis.style.setProperty('grid-template-columns','repeat(2,minmax(0,1fr))','important');
    }else{kpis.removeAttribute('style');}
  }
}
window.addEventListener("resize",()=>{fitStage();if(lastData){renderKpis(lastData,currentFeatures(),operationalContextFor(lastData),lastAnalytics);renderChart(lastData,chartZoomState.lastEvents||[],lastAnalytics)}});

async function loadMlAudit(){
  try{
    const response=await fetch('/ml_model_audit_latest.json',{cache:'no-store'});
    if(!response.ok)return;
    mlAudit=await response.json();
    const ap=v=>v===null||v===undefined?'—':fmt(v,3);
    const rows=$('mlReportRows');
    if(rows)rows.innerHTML=(mlAudit.queues||[]).map(q=>`<tr><td>Inst. ${q.instancia} · ${q.fila}</td><td>${q.test_positive_episodes_approx??'—'}</td><td>${ap(q.rule_current_congestion?.average_precision)}</td><td>${ap(q.existing_shadow?.average_precision)}</td><td>${ap(q.candidate_hist_gradient_boosting?.average_precision)}</td></tr>`).join('');
    const reportDate=new Date(mlAudit.generated_at);
    if($('mlReportTitle'))$('mlReportTitle').textContent=Number.isFinite(+reportDate)?`Avaliação de ${reportDate.toLocaleDateString('pt-BR')}`:'Avaliação semanal sem data válida';
    if($('mlWeeklyAssessment'))$('mlWeeklyAssessment').textContent=`${mlAudit.model_review||'Homologação pendente.'} ${mlAudit.new_model_assessment||''} Próxima revisão: ${mlAudit.next_review_at?new Date(mlAudit.next_review_at).toLocaleDateString('pt-BR'):'aguardando'}.`;
    const evaluated=(mlAudit.queues||[]).filter(q=>q.candidate_hist_gradient_boosting?.average_precision!==undefined);
    const better=evaluated.filter(q=>q.candidate_hist_gradient_boosting.average_precision>q.existing_shadow?.average_precision);
    const summary=$('mlCandidateSummary');
    if(summary)summary.textContent=`Candidato HistGradientBoosting: ${evaluated.length}/${(mlAudit.queues||[]).length} filas avaliáveis; ${better.length} com melhor precisão média. Sem validação prospectiva.`;
    const badge=$('mlValidationBadge');
    if(badge)badge.title=`Modelo operacional não homologado. Auditoria retrospectiva de ${new Date(mlAudit.generated_at).toLocaleDateString('pt-BR')}; falta validação com incidentes confirmados e acompanhamento prospectivo.`;
  }catch(_error){/* A ausência do relatório não altera o ML Shadow. */}
}
async function init(){
  if(appInitialized||initializing||!window.toiAuthenticated)return;
  initializing=true;
  try {
    const catalog=await api("/api/queues");
    if(!Array.isArray(catalog)||!catalog.length)throw new Error('Catálogo temporariamente indisponível');
    queues=catalog.map(q=>({...q,instancia:String(q.instancia),codigo:String(q.codigo)}));
  } catch (error) {
    const mode=$("temporalModeLabel");
    if(mode){mode.textContent="CONEXÃO TEMPORARIAMENTE INDISPONÍVEL · NOVA TENTATIVA AUTOMÁTICA";mode.className="temporal-mode-label warn"}
    clearTimeout(initRetryTimer);initRetryTimer=setTimeout(init,5000);
    return;
  } finally {
    initializing=false;
  }
  appInitialized=true;
  clearTimeout(initRetryTimer);
  fitStage();bindMainChartInteraction();
  const mlCard=$('mlShadowCard'),mlDialog=$('mlReportDialog');
  const openMlReport=()=>{if(!mlDialog.open)mlDialog.showModal();};
  mlCard?.addEventListener('click',openMlReport);
  mlCard?.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();openMlReport();}});
  $('mlReportClose')?.addEventListener('click',()=>mlDialog.close());
  Promise.allSettled([loadOperationalContextConfig(),loadSystemHealthSnapshot(true),loadGatewayHealthSnapshot(true),loadAgirouteSnapshot(true),loadMlAudit()]);
  const insts=[...new Set(queues.map(q=>q.instancia))].sort((a,b)=>+a-+b);
  $("instanceSelect").innerHTML=insts.map(x=>`<option value="${x}">${x}</option>`).join("");
  $("instanceSelect").value=insts.includes("4")?"4":insts[0];
  fillQueues("0249700009");
  window.dispatchEvent(new Event('toi-selection-ready'));
  $("instanceSelect").onchange=()=>{fillQueues();loadNow(true)};
  $("queueSelect").onchange=()=>loadNow(true);
  $("allGatewaysBtn")?.addEventListener("click",()=>openEntityPeriodAnalytics("gateways"));
  $("allTrunksBtn")?.addEventListener("click",()=>openEntityPeriodAnalytics("trunks"));
  $("navGatewayAnalytics")?.addEventListener("click",()=>openEntityPeriodAnalytics("gateways"));
  $("navTrunkAnalytics")?.addEventListener("click",()=>openEntityPeriodAnalytics("trunks"));
  $("infraModalClose")?.addEventListener("click",closeInfraModal);
  $("infraInventoryModal")?.addEventListener("click",e=>{
    if(e.target?.dataset?.infraModalClose==="1")closeInfraModal();
  });

  $("customPeriodBtn")?.addEventListener("click",openCustomPeriodModal);
  $("customPeriodClose")?.addEventListener("click",closeCustomPeriodModal);
  $("customPeriodCancel")?.addEventListener("click",closeCustomPeriodModal);
  $("customPeriodApply")?.addEventListener("click",applyCustomPeriod);
  $("customPeriodModal")?.addEventListener("click",e=>{
    if(e.target?.dataset?.customClose==="1")closeCustomPeriodModal();
  });

  $("openStuckAnalytics")?.addEventListener("click",()=>openManagerialAnalytics("stuck"));
  $("openCapacityAnalytics")?.addEventListener("click",()=>openManagerialAnalytics("capacity"));
  $("openIncidentHistory")?.addEventListener("click",openIncidentHistory);
  $("operationalPressurePanel")?.addEventListener("click",()=>openManagerialAnalytics("pressure"));
  $("navStuckAnalytics")?.addEventListener("click",()=>openManagerialAnalytics("stuck"));
  $("navCapacityAnalytics")?.addEventListener("click",()=>openManagerialAnalytics("capacity"));
  $("managerialAnalyticsClose")?.addEventListener("click",closeManagerialAnalytics);
  $("managerialAnalyticsModal")?.addEventListener("click",e=>{
    if(e.target?.dataset?.analyticsClose==="1")closeManagerialAnalytics();
  });

  document.addEventListener("keydown",e=>{
    if(e.key!=="Escape")return;
    if($("infraInventoryModal")?.classList.contains("open"))closeInfraModal();
    if($("customPeriodModal")?.classList.contains("open"))closeCustomPeriodModal();
    if($("managerialAnalyticsModal")?.classList.contains("open"))closeManagerialAnalytics();
  });
  document.querySelectorAll(".periods button[data-p]").forEach(b=>b.onclick=()=>{
    document.querySelectorAll(".periods button").forEach(x=>x.classList.remove("active"));
    b.classList.add("active");currentPeriod=b.dataset.p;livePeriod=false;$("periodSelect").value=currentPeriod;customRange=null;loadNow(true);
  });
  $("periodSelect").onchange=event=>{
    const selected=event.target.value;
    if(selected==='custom'){openCustomPeriodModal();return;}
    livePeriod=selected==='live';currentPeriod=livePeriod?'15m':selected;customRange=null;
    if(livePeriod){refreshSeconds=5;$("refreshSelect").value='5';}
    document.querySelectorAll('.periods button[data-p]').forEach(button=>button.classList.toggle('active',button.dataset.p===currentPeriod));
    loadNow(true);schedule();
  };
  $("refreshSelect").onchange=event=>{refreshSeconds=Number(event.target.value);schedule();};
  schedule();
  loadNow(true);
}
function fillQueues(prefer){
  const list=queues.filter(q=>q.instancia===$("instanceSelect").value);
  $("queueSelect").innerHTML=list.map(q=>`<option value="${q.codigo}">${q.nome} (${q.codigo})${q.collected===false?' · inventário, sem coleta ao vivo':''}</option>`).join("");
  if(prefer&&list.some(q=>q.codigo===prefer))$("queueSelect").value=prefer;
}
function renderCatalogPreview(queue,status){
  const panel=$('catalogQueuePreview');if(!panel)return;
  panel.hidden=!queue;
  if(!queue)return;
  $('catalogQueueName').textContent=`${queue.nome} (${queue.codigo}) · Instância ${queue.instancia}`;
  const live=status?.available===true;
  $('catalogQueueSource').textContent=live
    ?`Portal Orbe atualizado às ${String(status.sample_at||'').slice(11,19)} · leitura atual`
    :status?`Portal Orbe: ${status.reason||'última leitura antiga ou indisponível'}`:'Consultando o Portal Orbe…';
  $('catalogP34').textContent=live&&status.p34!==null&&status.p34!==undefined&&Number.isFinite(Number(status.p34))?`${fmt(status.p34,2)}%`:'—';
  $('catalogP135').textContent=live&&status.p135!==null&&status.p135!==undefined&&Number.isFinite(Number(status.p135))?`${fmt(status.p135,2)}%`:'—';
  panel.classList.toggle('stale',!live);
}
// Navigation requested only after the Radar server confirms persisted acknowledgment.
window.toiOpenRadarAnalysis=async incident=>{
  if(!incident?.ack_at)throw new Error('Registre Ciente antes de abrir a análise.');
  const inst=String(incident.instancia),code=String(incident.codigo||'');
  if(!queues.some(q=>String(q.instancia)===inst))throw new Error('Instância ausente do catálogo atual. O registro do incidente permanece no Radar.');
  if(code&&!queues.some(q=>String(q.instancia)===inst&&String(q.codigo)===code))throw new Error('Fila ausente do catálogo atual. O registro do incidente permanece no Radar.');
  $("instanceSelect").value=inst;fillQueues(code);
  await loadNow(true);
  if(!lastAnalytics||lastAnalyticsKey!==analyticsSelectionKey(inst,$("queueSelect").value))throw new Error('Análise ainda indisponível para esta seleção. Tente novamente.');
  if(incident.source==='GATEWAY')await openEntityPeriodAnalytics('gateways');
  else openIncidentHistory();
};
function schedule(){clearTimeout(refreshTimer);if(refreshSeconds&&window.toiAuthenticated)refreshTimer=setTimeout(async()=>{try{await loadNow(livePeriod)}finally{schedule()}},refreshSeconds*1000)}
function analyticsSelectionKey(inst,code){
  return [inst,code,currentPeriod,customRange?.start||"",customRange?.end||""].join("|");
}
function analyticsRefreshTtlMs(){
  const m=periodMinutes();
  if(m<=120)return 30000;
  if(m<=1440)return 120000;
  return 300000;
}

function analyticsCacheGet(key){
  const x=analyticsCache.get(key);
  if(!x)return null;
  if(Date.now()-x.at>analyticsRefreshTtlMs()){analyticsCache.delete(key);return null}
  return x;
}
function analyticsCachePut(key,data){
  analyticsCache.set(key,{at:Date.now(),data});
  if(analyticsCache.size>16){
    const oldest=[...analyticsCache.entries()].sort((a,b)=>a[1].at-b[1].at).slice(0,analyticsCache.size-16);
    oldest.forEach(([k])=>analyticsCache.delete(k));
  }
}
function periodSemantics(a=lastAnalytics){return a?.period_semantics||{}}
function periodCoveragePct(a=lastAnalytics){
  const sem=periodSemantics(a),cov=a?.coverage?.queue||{};
  return finiteNumber(sem.historical_coverage_pct??cov.data_coverage_pct??cov.coverage_pct);
}
function periodIsPartial(a=lastAnalytics){
  if(!a)return false;
  const sem=periodSemantics(a);
  if(sem.historical_status)return sem.historical_status!=="COMPLETE";
  const cov=a?.coverage?.queue||{};return !cov.complete;
}
function periodStatsCaption(a=lastAnalytics){
  if(!a)return "LIVE 15 min";
  return periodIsPartial(a)?`observado no período de ${periodLabel()}`:`${periodLabel()}`;
}

function analyticsUrlForPeriod(inst,code,period){
  return `/api/analytics/window?instancia=${encodeURIComponent(inst)}&codigo=${encodeURIComponent(code)}&period=${encodeURIComponent(period)}`;
}
function analyticsUrl(inst,code){
  const base=`/api/analytics/window?instancia=${encodeURIComponent(inst)}&codigo=${encodeURIComponent(code)}&period=${encodeURIComponent(currentPeriod)}`;
  if(currentPeriod!=="custom")return base;
  const start=customRange?.start||"",end=customRange?.end||"";
  return `${base}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`;
}
async function loadNow(forceAnalytics=false){
  const inst=$("instanceSelect").value,code=$("queueSelect").value;
  if(!inst||!code||!window.toiAuthenticated)return;
  const inventoryOnly=queues.find(q=>q.instancia===inst&&q.codigo===code&&q.collected===false);
  renderCatalogPreview(inventoryOnly,null);
  if(inventoryOnly)api(`/api/portal-queue-status?instancia=${encodeURIComponent(inst)}&codigo=${encodeURIComponent(code)}`)
    .then(status=>{if($("instanceSelect").value===inst&&$("queueSelect").value===code)renderCatalogPreview(inventoryOnly,status);})
    .catch(()=>{if($("instanceSelect").value===inst&&$("queueSelect").value===code)renderCatalogPreview(inventoryOnly,{available:false,reason:'fonte indisponível'});});
  const selectionKey=analyticsSelectionKey(inst,code);
  if(overviewInFlight.has(selectionKey))return;
  const seq=selectionKey!==activeLoadKey?++loadSeq:loadSeq;
  activeLoadKey=selectionKey;
  const selectionChanged=selectionKey!==lastAnalyticsKey;
  const cacheEntry=analyticsCacheGet(selectionKey);
  if(cacheEntry){
    lastAnalytics=cacheEntry.data;lastAnalyticsKey=selectionKey;lastAnalyticsLoadAt=cacheEntry.at;
  }else if(selectionChanged&&forceAnalytics){
    lastAnalytics=null;
  }
  const analyticsExpired=(Date.now()-lastAnalyticsLoadAt)>=analyticsRefreshTtlMs();
  const needAnalytics=(livePeriod||(!cacheEntry&&(forceAnalytics||selectionChanged||!lastAnalytics||analyticsExpired)))&&analyticsInFlightKey!==selectionKey;

  // R1.2.4: cockpit LIVE e histórico selecionado são contratos independentes.
  // O overview de 15m nunca herda cobertura/completude de 7/14/30d.
  const overviewPeriod="15m";

  overviewInFlight.add(selectionKey);
  try{
    Promise.allSettled([loadSystemHealthSnapshot(false),loadGatewayHealthSnapshot(false),loadAgirouteSnapshot(false)]);
    const d=await api(`/api/overview?instancia=${encodeURIComponent(inst)}&codigo=${encodeURIComponent(code)}&period=${encodeURIComponent(overviewPeriod)}`);
    if(seq!==loadSeq||selectionKey!==analyticsSelectionKey($("instanceSelect").value,$("queueSelect").value))return;
    lastData=d;
    // Current 15-minute values must paint before the historical query finishes.
    render(d,lastAnalyticsKey===selectionKey?lastAnalytics:null);

    if(!needAnalytics)return;
    const mode=$("temporalModeLabel");
    if(mode){mode.textContent=`CARREGANDO ${periodLabel().toUpperCase()}…`;mode.className="temporal-mode-label warn"}

    analyticsInFlightKey=selectionKey;
    void (async()=>{
    try{
      const a=await api(analyticsUrl(inst,code)+(livePeriod?'&cache_bypass=1':''));
      if(seq!==loadSeq||selectionKey!==analyticsSelectionKey($("instanceSelect").value,$("queueSelect").value))return;
      lastAnalytics=a;lastAnalyticsKey=selectionKey;lastAnalyticsLoadAt=Date.now();analyticsCachePut(selectionKey,a);
      render(lastData||d,a);
    }catch(e){
      if(seq!==loadSeq)return;
      console.error(e);
      render(lastData||d,lastAnalyticsKey===selectionKey?lastAnalytics:null);
      if(mode){mode.textContent="ANÁLISE TEMPORAL INDISPONÍVEL";mode.className="temporal-mode-label bad"}
    }finally{
      if(analyticsInFlightKey===selectionKey)analyticsInFlightKey=null;
    }
    })();
  }catch(e){
    if(seq===loadSeq){
      console.error(e);
      const mode=$("temporalModeLabel");
      if(mode){mode.textContent="DADOS LIVE INDISPONÍVEIS";mode.className="temporal-mode-label bad"}
    }
  }finally{overviewInFlight.delete(selectionKey);}
}
function currentFeatures(){return lastAnalytics?.queue_summary||lastData?.period_summary||{}}

function render(d,a=lastAnalytics){
  const l=d.latest||{},g=d.diagnosis||{},w=a?.queue_summary||d.period_summary||{},cap=d.capacity||{},st=d.stuck||{};
  const opctx=operationalContextFor(d);
  // R1.2.4: este gate pertence ao diagnóstico LIVE (overview 15m), não ao histórico selecionado.
  const coverageGate=renderCoverageGate(d,opctx);
  renderFreshnessHeader(d);
  renderSystemHealthExecutive();
  renderAgiroutePanel();
  $("updatedAt").classList.toggle("fresh-live",d.freshness?.overall==="LIVE");
  $("updatedAt").classList.toggle("fresh-warn",d.freshness?.overall==="PARCIAL");
  $("updatedAt").classList.toggle("fresh-stale",d.freshness?.overall==="STALE");
  $("healthScore").textContent=opctx.afterHours?"—":(g.health_score??"—");
  $("healthLabel").textContent=opctx.afterHours?"FORA JANELA":(g.severity==="CRÍTICO"?"CRÍTICO":g.severity==="ATENÇÃO"?"ATENÇÃO":"ESTÁVEL");
  $("healthLabel").classList.toggle("context-label",opctx.afterHours);
  const delta=w.p135_pct?.delta||0;$("trendLabel").innerHTML=opctx.afterHours?`Contexto: ${opctx.windowText||"fora da janela"}`:`Tendência: ${delta>1?'<b style="color:#ff4148">↑</b>':delta<-1?'<b style="color:#63d17a">↓</b>':'→'}`;
  $("diagTitle").textContent=opctx.afterHours?"Fora da janela operacional — observabilidade permanece ativa":(g.title||"Sem diagnóstico");
  const status=clean(l.process_status),active=clean(l.active);
  const parts=[];if(status)parts.push(`Fila ${status}.`);if(active)parts.push(`Ativa: ${active}.`);if(g.p135_avg!==undefined)parts.push(`P135 médio ${fmt(g.p135_avg)}%.`);
  if(cap.current_max!==null&&cap.current_max!==undefined)parts.push(`Ocupação relevante de troncos ${fmt(cap.current_max)}%.`);if(d.freshness?.sources?.infra?.usable)parts.push("Gateways avaliados na janela.");
  else parts.push(`Infra ${d.freshness?.sources?.infra?.status||"indisponível"}: não considerada.`);
  if(d.quality_gate?.score!==undefined)parts.push(`Qualidade das fontes ${d.quality_gate.score}%.`);
  if(opctx.afterHours)parts.unshift(`Contexto operacional: ${opctx.contextText||"fora da janela operacional"}; ausência de discagem e agentes pode ser esperada.`);
  else if(coverageGate.pct<70)parts.push(`Cobertura LIVE recente ${fmt(coverageGate.pct,2)}%: diagnóstico corrente com confiança reduzida.`);
  $("diagText").textContent=parts.join(" ");
  $("confidence").textContent=opctx.afterHours?"—":(g.confidence===undefined?"—":`${g.confidence}%`);
  $("confidenceBar").style.width=opctx.afterHours?"0%":`${g.confidence||0}%`;
  $("confidenceBar").className=opctx.afterHours?"context":(g.confidence>=80?"ok":g.confidence>=50?"warn":"bad");
  try{renderInfrastructureAlerts()}catch(err){
    console.warn("Gateway Health alerts non-fatal:",err);
    const f=$("factors");
    if(f)f.innerHTML='<div class="infra-alert neutral"><b>N/C</b><span>Gateway Health indisponível; cockpit principal preservado.</span></div>';
  }

  const queueFresh=["LIVE","ATRASO"].includes(d.freshness?.sources?.queue?.status);
  const p135=queueFresh?(+l.p135_pct||0):null,p34=queueFresh?(+l.p34_pct||0):null,crit=queueFresh&&Math.max(p135||0,p34||0)>=20;
  renderStateTriad(d,crit,opctx);
  try{renderGatewayExecutiveChip()}catch(err){
    console.warn("Gateway Health chip non-fatal:",err);
    setStateChip("gatewayStatusChip","GW","N/C","neutral");
  }
  $("incidentState").textContent=opctx.afterHours?"● Fora da janela operacional":crit?"● Incidente ativo":"● Sem incidente crítico ativo";
  $("incidentState").style.color=opctx.afterHours?"#74a8bd":crit?"#ff3d45":"#62d279";
  const inc=(d.recent_incidents||[]).find(x=>x.status==="ATIVO");
  $("incidentStart").textContent=inc?inc.start.slice(11,16):"—";
  $("incidentDuration").textContent=inc?duration(inc.start,l.ts):"—";
  if($("incidentTiming"))$("incidentTiming").hidden=!inc;
  const cov=a?.coverage?.queue||d.period_coverage||{};
  const sem=a?.period_semantics||{};
  const hasLiveHistory=(d.series||[]).length>0;
  const covPct=sem.historical_coverage_pct??cov.data_coverage_pct??cov.coverage_pct??0;
  const observedMin=sem.observed_useful_minutes??cov.available_minutes??cov.span_minutes??0;
  const histComplete=a?Boolean(sem.historical_complete??cov.complete):Boolean(cov.complete);
  const coverageText=a
    ?(histComplete
      ?`${periodLabel()} · período observado completo · ${n0(cov.samples)} amostras`
      :`${periodLabel()} solicitados · ${humanMinutes(observedMin)} observados · ${fmt(covPct,2)}% cobertura útil`)
    :(hasLiveHistory?`Histórico LIVE · ${n0(d.series.length)} amostras observadas`:`Aguardando primeira amostra da fila`);
  $("coverageLabel").textContent=coverageText;
  $("coverageLabel").classList.toggle("partial",a?!histComplete:false);
  const temporalMode=$("temporalModeLabel");
  if(temporalMode){
    const profile=a?.mode==="PROFILE_24H";
    temporalMode.hidden=Boolean(a&&profile);
    temporalMode.textContent=a?(profile?"":"CRONOLOGIA REAL"):(hasLiveHistory?"CRONOLOGIA LIVE":"AGUARDANDO AMOSTRA");
    temporalMode.className=`temporal-mode-label ${a?(profile?"profile":"chrono"):"warn"}`;
  }
  const evolutionTitle=$("evolutionTitle");
  if(evolutionTitle){
    const profile=a?.mode==="PROFILE_24H";
    evolutionTitle.textContent=!a?(hasLiveHistory?"EVOLUÇÃO LIVE · 15 MIN":"EVOLUÇÃO LIVE · 15 MIN · AGUARDANDO AMOSTRA"):(profile?`PERFIL HORÁRIO AGREGADO · ${periodLabel().toUpperCase()}`:`EVOLUÇÃO TEMPORAL SINCRONIZADA · ${periodLabel().toUpperCase()}`);
    evolutionTitle.title=profile?"Perfil horário agregado: consolida as amostras realmente observadas do período por horário do dia; cada ponto representa uma faixa de 15 minutos.":"Cronologia real das amostras observadas no período selecionado.";
  }
  $("operationalContextLabel").textContent=opctx.configured
    ? (opctx.afterHours?`FORA DA JANELA · ${opctx.windowText}`:`JANELA OPERACIONAL · ${opctx.windowText}`)
    : "";
  $("operationalContextLabel").className=`operational-context-label ${opctx.afterHours?"outside":"inside"}`;

  renderKpis(d,w,opctx,a);
  renderAgents(l,d.freshness?.sources?.queue);
  const inventoryInfra=d.infra_all||d.infra||{};
  renderTrunks(inventoryInfra.trunks||[],d.freshness?.sources?.infra,opctx);
  renderGateways(inventoryInfra.gateways||[],d.freshness?.sources?.infra);
  renderDialer(d.dialer||{},d.data_status?.dialer,d.freshness?.sources?.dialer);
  renderStuck(st.quantity,a?.stuck_analytics||d.stuck_summary||{},d.freshness?.sources?.stuck_channels,a);
  let events=[];
  if(a?.mode!=="PROFILE_24H"){
    const eventSeries=a?.series?.queue||d.series||[];
    const eventTrunks=a?.series?.trunk||d.infra_series?.trunk||[];
    events=deriveEvents(eventSeries,eventTrunks);
  }
  renderPeriodHighlights(d,a,events,opctx);
  renderChart(d,events,a);
  renderRecs([...gatewayInvestigationRecommendations(d.gateway_investigation),...(a?.operational_recommendations||a?.operational_pressure?.recommendations||d.recommendations||[])],opctx);renderCapacity(cap,d,opctx,a);renderIncidents(reconcileRecentIncidents(a,d.recent_incidents||[]));renderMlShadow(d,opctx);
}
function analyticsSparkValues(a,key){
  if(!a)return [];
  if(a.mode==="PROFILE_24H"){
    const p=a.profile||[];
    if(key==="__trunk")return p.map(x=>finiteNumber(x.trunk_occupancy?.p95)).filter(v=>v!==null);
    if(key==="__stuck")return p.map(x=>finiteNumber(x.stuck_quantity?.mean)).filter(v=>v!==null);
    return p.map(x=>finiteNumber(x?.[key]?.mean)).filter(v=>v!==null);
  }
  if(key==="__trunk")return (a.series?.trunk||[]).map(v=>finiteNumber(v.occupancy)).filter(v=>v!==null);
  if(key==="__stuck")return (a.series?.stuck||[]).map(v=>+v.quantity||0);
  return (a.series?.queue||[]).map(v=>+v[key]||0);
}
function pressureIndexMeaning(score){
  const v=finiteNumber(score);
  if(v===null)return {label:"N/C",range:"—",text:"Evidência insuficiente para interpretar a pressão desta dimensão."};
  if(v>=85)return {label:"CRÍTICA",range:"85–100",text:v>=99.5?"Pressão crítica; o índice atingiu o teto da escala. Isso NÃO significa P34/P135 de 100%.":"Pressão crítica: o valor que 95% das leituras não ultrapassaram, o pico e/ou o tempo acima de 20% indicam exposição severa no período."};
  if(v>=70)return {label:"ELEVADA",range:"70–84",text:"Pressão elevada: o valor que 95% das leituras não ultrapassaram, o pico e/ou o tempo acima de 20% mostram exposição relevante no período."};
  if(v>=45)return {label:"MODERADA",range:"45–69",text:"Pressão moderada: houve aproximação ou ultrapassagem relevante do crítico, mas sem severidade/persistência suficiente para a faixa elevada."};
  return {label:"BAIXA",range:"0–44",text:"Pressão baixa: o sinal foi limitado na janela e não sustenta exposição crítica relevante ou persistente."};
}
function pressureTone(level){
  const v=String(level||'').toUpperCase();
  if(v.includes('CRÍT'))return 'bad';
  if(v.includes('ELEV')||v.includes('ALTA'))return 'bad';
  if(v.includes('MODER'))return 'warn';
  if(v.includes('BAIX'))return 'ok';
  return 'neutral';
}
function renderTelecomPressure(d,a=lastAnalytics){
  const el=$("telecomPressure");if(!el)return;
  const po=a?.operational_pressure||{},score=finiteNumber(po.score),level=po.level||'EVIDÊNCIA INSUFICIENTE';
  const selected=a?periodLabel():'AGORA',cov=finiteNumber(po.evidence_coverage_pct);
  const contributors=(po.contributors||[]).filter(x=>x.scored!==false).slice(0,2).map(x=>`${x.name}: ${String(x.level||'').toLowerCase()}`).join(' · ');
  const fill=score===null?0:Math.max(0,Math.min(100,score));
  const ringColor=score===null?'#657681':score<=59?'#65d27a':score<=80?'#ffad35':'#ff5961';
  const executive=po.executive_summary||'';
  const detail=executive?esc(executive):`${po.primary_factor?`Principal fator: ${esc(po.primary_factor)}. `:''}${contributors?esc(contributors):'Aguardando evidência suficiente.'}`;
  el.className='telecom-pressure-body';
  el.innerHTML=`<div class="pressure-ring" style="--pressure-fill:${fill}%;--pressure-color:${ringColor}" role="img" aria-label="Índice de pressão ${score===null?'indisponível':fmt(score,1)+' de 100'}"><div class="pressure-ring-inner"><strong>${score===null?'—':fmt(score,2)}</strong><small>DE 100</small></div></div>
    <div class="pressure-readout"><div class="telecom-pressure-scope">PERÍODO · ${esc(selected)}</div><b>${esc(level)}</b><p title="${detail}">${detail}</p><small>${cov!==null?`Cobertura das fontes ${fmt(cov,2)}%`:'Cobertura das fontes N/C'}</small></div>`;
  el.title='Índice observacional de 0 a 100, calculado com os sinais do período. O valor não é percentual de congestionamento nem previsão do ML Shadow.';
}

function renderKpis(d,w,opctx=operationalContextFor(d),a=lastAnalytics){
  renderTelecomPressure(d,a);
  const rawLatest=d.latest||{},st=d.stuck||{};
  const queueFresh=["LIVE","ATRASO"].includes(d.freshness?.sources?.queue?.status);
  const stuckFresh=["LIVE","ATRASO"].includes(d.freshness?.sources?.stuck_channels?.status);
  const l=queueFresh?rawLatest:{};
  const ss=a?.stuck_analytics||d.stuck_summary||{};
  const longPrimary=periodUsesHistoricalPrimary(a);
  const activityMax=Math.max(finiteNumber(w.calls_dialing?.max)||0,finiteNumber(w.calls_in_attendance?.max)||0);
  const historicalActivity=activityMax>0,hitNowApplicable=!opctx.afterHours&&historicalActivity;
  const p34NowVal=queueFresh?finiteNumber(l.p34_pct):null,p135NowVal=queueFresh?finiteNumber(l.p135_pct):null;
  const p34Period=finiteNumber(w.p34_pct?.max),p135Period=finiteNumber(w.p135_pct?.max);
  const ca=a?.congestion_analytics||{};
  const queueCov=sourceCoverageText(a,"queue","fila");
  const stuckCov=sourceCoverageText(a,"stuck","canais");
  const selected=periodLabel();
  const cardScope=longPrimary?`<em>(${selected})</em>`:'<em>(agora)</em>';
  const p34Main=longPrimary?p34Period:p34NowVal,p135Main=longPrimary?p135Period:p135NowVal;
  const congestionCard=`<article class="panel kpi congestion-dual-kpi ${longPrimary?'period-primary':''}">
    <div class="label">CONGESTIONAMENTO ${cardScope}</div>
    <div class="congestion-kpi-grid">
      <div class="p34"><span>P34 ${longPrimary?'· pico':'· agora'}</span><strong>${p34Main===null?'—':pct(p34Main)}</strong><small>${longPrimary?`Méd. ${pct(w.p34_pct?.mean)} · 95% das leituras ficaram até ${pct(w.p34_pct?.p95)} · ≥20% ${humanMinutes(ca?.p34?.minutes_over_20)} / ${n0(ca?.p34?.episodes_over_20)} ep. · agora ${pct(p34NowVal)}`:`Pico ${selected} ${pct(w.p34_pct?.max)} · méd. ${pct(w.p34_pct?.mean)}`}${queueCov}</small></div>
      <div class="p135"><span>P135 ${longPrimary?'· pico':'· agora'}</span><strong>${p135Main===null?'—':pct(p135Main)}</strong><small>${longPrimary?`Méd. ${pct(w.p135_pct?.mean)} · 95% das leituras ficaram até ${pct(w.p135_pct?.p95)} · ≥20% ${humanMinutes(ca?.p135?.minutes_over_20)} / ${n0(ca?.p135?.episodes_over_20)} ep. · agora ${pct(p135NowVal)}`:`Pico ${selected} ${pct(w.p135_pct?.max)} · méd. ${pct(w.p135_pct?.mean)}`}${queueCov}</small></div>
    </div>
  </article>`;
  const hit5Main=longPrimary?(historicalActivity?pct(w.hit_rate5?.mean):'—'):(hitNowApplicable?pct(l.hit_rate5):'—');
  const hit15Main=longPrimary?(historicalActivity?pct(w.hit_rate15?.mean):'—'):(hitNowApplicable?pct(l.hit_rate15):'—');
  const hit5Sub=historicalActivity?(longPrimary?`Média das amostras observadas · mínimo ${pct(w.hit_rate5?.min)} · 95% das leituras ficaram até ${pct(w.hit_rate5?.p95)} · agora ${pct(l.hit_rate5)}${queueCov}`:`Média das amostras de ${selected}: ${pct(w.hit_rate5?.mean)} · 95% das leituras ficaram até ${pct(w.hit_rate5?.p95)}${queueCov}`):`N/A · sem atividade observada${queueCov}`;
  const hit15Sub=historicalActivity?(longPrimary?`Média das amostras observadas · mínimo ${pct(w.hit_rate15?.min)} · 95% das leituras ficaram até ${pct(w.hit_rate15?.p95)} · agora ${pct(l.hit_rate15)}${queueCov}`:`Média das amostras de ${selected}: ${pct(w.hit_rate15?.mean)} · 95% das leituras ficaram até ${pct(w.hit_rate15?.p95)}${queueCov}`):`N/A · sem atividade observada${queueCov}`;
  const stuckMain=longPrimary?(finiteNumber(ss?.max)===null?'—':n0(ss.max)):(stuckFresh?n0(st.quantity):'—');
  const stuckSub=a?(longPrimary?`máx simultâneo no período · ${n0(ss.episodes_count)} episódios · ${humanMinutes(ss.affected_minutes)} com ocorrência · maior episódio ${humanMinutes(ss.max_episode_minutes)} · exposição ${fmt(ss.channel_minutes,0)} canal-min · agora ${stuckFresh?n0(st.quantity):'—'}${stuckCov}`:`${n0(ss.episodes_count)} episódios · ${humanMinutes(ss.affected_minutes)} com ocorrência · máx simultâneo ${n0(ss.max)}${stuckCov}`):`Histórico ${selected} carregando…`;
  const defs=[
    [`Hit Rate 5' ${cardScope}`,hit5Main,"orange",hit5Sub,"hit_rate5","#ff9818"],
    [`Hit Rate 15' ${cardScope}`,hit15Main,"orange",hit15Sub,"hit_rate15","#ffb316"],
    [`Chamadas Discando ${cardScope}`,longPrimary?n0(w.calls_dialing?.mean):n0(l.calls_dialing),"blue",longPrimary?`média simultânea · 95% das leituras ficaram até ${n0(w.calls_dialing?.p95)} · pico simultâneo ${n0(w.calls_dialing?.max)} · agora ${n0(l.calls_dialing)} · total iniciado N/D na fonte atual${queueCov}`:`Média simultânea ${selected}: ${n0(w.calls_dialing?.mean)} · 95% das leituras ficaram até ${n0(w.calls_dialing?.p95)} · pico ${n0(w.calls_dialing?.max)} · total iniciado N/D${queueCov}`,"calls_dialing","#199fe3"],
    [`Em Atendimento ${cardScope}`,longPrimary?n0(w.calls_in_attendance?.mean):n0(l.calls_in_attendance),"teal",longPrimary?`média do período · 95% das leituras ficaram até ${n0(w.calls_in_attendance?.p95)} · pico ${n0(w.calls_in_attendance?.max)} · agora ${n0(l.calls_in_attendance)}${queueCov}`:`Méd. ${selected}: ${n0(w.calls_in_attendance?.mean)} · 95% das leituras ficaram até ${n0(w.calls_in_attendance?.p95)} · pico ${n0(w.calls_in_attendance?.max)}${queueCov}`,"calls_in_attendance","#25a9bf"],
  ];
  $("kpis").innerHTML=congestionCard+defs.map((x,i)=>`<article class="panel kpi ${x[2]} ${longPrimary?'period-primary':''}"><div class="label">${x[0]}</div><div class="value">${x[1]}</div><div class="sub">${x[3]}</div><canvas id="sp${i}"></canvas></article>`).join("");
  defs.forEach((x,i)=>{
    let vals=analyticsSparkValues(a,x[4]);
    if(!vals.length){
      if(x[4]==="__stuck") vals=(d.stuck_series||[]).map(v=>+v.quantity||0);
      else vals=(d.series||[]).map(v=>+v[x[4]]||0);
    }
    spark($(`sp${i}`),vals,x[5]);
  });
}
function spark(c,vals,color){
  if(!c||!vals.length)return;const r=c.getBoundingClientRect(),d=window.devicePixelRatio||1;c.width=r.width*d;c.height=r.height*d;const x=c.getContext("2d");x.setTransform(d,0,0,d,0,0);
  const W=r.width,H=r.height,mn=Math.min(...vals),mx=Math.max(...vals),sp=Math.max(1,mx-mn);x.clearRect(0,0,W,H);x.strokeStyle=color;x.lineWidth=1.2;x.beginPath();
  vals.forEach((v,i)=>{const px=i/(vals.length-1||1)*W,py=H-2-(v-mn)/sp*(H-5);i?x.lineTo(px,py):x.moveTo(px,py)});x.stroke();
  const g=x.createLinearGradient(0,0,0,H);g.addColorStop(0,color+"55");g.addColorStop(1,color+"00");x.lineTo(W,H);x.lineTo(0,H);x.closePath();x.fillStyle=g;x.fill();
}
function renderAgents(l,fresh){
  const valid=["LIVE","ATRASO"].includes(fresh?.status);
  if(!valid) l={};
  const a=[["#5ec46b","Disponíveis",+l.agents_available||0],["#1c9fd8","Falando",+l.agents_talking||0],["#d89918","ACW",+l.agents_acw||0],["#8d70d8","Pausados",+l.agents_paused||0],["#7775b9","Outros",+l.agents_other||0],["#24aab7","Discando",+l.agents_dialing||0]],tot=a.reduce((s,x)=>s+x[2],0);
  $("agentTotal").textContent=valid?n0(tot):"—";$("agentTotalCaption").textContent=valid?n0(tot):"—";
  $("agents").innerHTML=a.map(x=>`<div class="agent-row"><i class="sw" style="background:${x[0]}"></i><span>${x[1]}</span><b>${x[2]}</b><span class="pct">${tot?fmt(x[2]/tot*100,2):"0,00"}%</span></div>`).join("");
}

function infraInstanceLabel(){
  return String($("instanceSelect")?.value||lastData?.latest?.instancia||"—");
}
function inventoryMetaText(visible,total,label){
  if(!total)return "sem itens";
  return total>visible?`${visible} de ${total}`:`${total} ${label}`;
}
function normalizeInfraRows(rows=[]){
  return Array.isArray(rows)?rows.filter(x=>x&&x.entity):[];
}
function renderInfraModal(kind){
  const d=lastData||{};
  const isGateway=kind==="gateways";
  const inventoryInfra=d?.infra_all||d?.infra||{};
  const rows=normalizeInfraRows(isGateway?inventoryInfra.gateways:inventoryInfra.trunks);
  const fresh=d?.freshness?.sources?.infra||{};
  $("infraModalTitle").textContent=`${isGateway?"Gateways · estado operacional e capacidade":"Grupos de Troncos · ocupação e distribuição"} · Instância ${infraInstanceLabel()}`;
  $("infraModalSubtitle").textContent=isGateway
    ?"Telemetria atual + Ground Truth operacional versionado. Contexto semântico não altera aquisição, diagnóstico ou ações."
    :"Inventário completo de grupos de troncos entregue pelo overview atual para a Instância selecionada.";
  $("infraModalCount").textContent=`${rows.length} ${isGateway?(rows.length===1?"gateway":"gateways"):(rows.length===1?"grupo":"grupos")}`;
  const fs=String(fresh?.status||"N/C");
  $("infraModalFreshness").textContent=`INFRA ${fs}${fresh?.age_s!==null&&fresh?.age_s!==undefined?` · ${Math.round(fresh.age_s)}s`:""}`;
  $("infraModalFreshness").className=`infra-modal-chip ${fresh?.usable?"ok":fs==="ATRASO"?"warn":"neutral"}`;

  let body="";
  if(!rows.length){
    body='<div class="infra-modal-empty">Nenhuma entidade foi entregue pela API para esta Instância.</div>';
  }else if(isGateway){
    body=`<div class="infra-full-head gateway"><span>Gateway</span><span>Ocupação</span><span>Estado operacional</span><span>ICMP</span><span>Papel / expectativa</span><span>Tratamento na capacidade</span></div>`+
      rows.map((x,i)=>{
        const occ=finiteNumber(x.occupancy),h=healthForGateway(x.entity);
        const status=gatewayStatusLabel(h),tone=gatewayTone(h?.status,h);
        const rtt=h?.rtt_p50_ms!==null&&h?.rtt_p50_ms!==undefined?` · ${Math.round(h.rtt_p50_ms)}ms`:"";
        const tip=esc(gatewayTooltip(h,status));
        return `<div class="infra-full-row gateway">
          <span><b>${i+1}</b>${esc(displayGatewayName(x.entity))}</span>
          <span><strong class="occ ${occ===null?"nc":occ>=90?"high":occ>=75?"mid":"ok"}">${pct(occ)}</strong></span>
          <span><i class="gateway-health-state ${tone}" title="${tip}">${esc(status)}</i></span>
          <span class="gateway-dimension" title="${tip}">${esc(gatewayReachabilityLabel(h))}${esc(rtt)}</span>
          <span class="gateway-dimension" title="${tip}">${esc(gatewayTrafficLabel(h))}</span>
          <span class="gateway-capacity-treatment ${tone}" title="${tip}">${esc(gatewayCapacityLabel(h))}</span>
        </div>`;
      }).join("");
  }else{
    const afterHours=operationalContextFor(d).afterHours;
    body=`<div class="infra-full-head trunk"><span>Grupo de Troncos</span><span>Ocupação</span><span>Canais</span><span>Status</span></div>`+
      rows.map((x,i)=>{
        const occ=finiteNumber(x.occupancy);
        const status=occ===null?"Sem dado":occ>0?"Em operação":"Sem ocupação";
        const statusCls=occ===null?"unknown":occ>0?"up":"down";
        return `<div class="infra-full-row trunk">
          <span><b>${i+1}</b>${x.entity}</span>
          <span><strong class="occ ${occ===null?"nc":occ>=90?"high":occ>=75?"mid":"ok"}">${pct(occ)}</strong></span>
          <span>${n0(x.busy)} / ${n0(x.total)}</span>
          <span class="infra-status-text"><i class="dot-status ${statusCls}"></i>${status}</span>
        </div>`;
      }).join("");
  }
  $("infraModalTable").innerHTML=body;
  const modal=$("infraInventoryModal");
  modal.classList.add("open");
  modal.setAttribute("aria-hidden","false");
  document.body.classList.add("infra-modal-open");
  setTimeout(()=>$("infraModalClose")?.focus(),0);
}
function closeInfraModal(){
  const modal=$("infraInventoryModal");
  if(!modal)return;
  modal.classList.remove("open");
  modal.setAttribute("aria-hidden","true");
  document.body.classList.remove("infra-modal-open");
}

function occupancyMeter(value){
  const occ=finiteNumber(value),tone=trunkPressureClass(occ);
  const width=occ===null?0:Math.max(0,Math.min(100,occ));
  return `<span class="occupancy-meter ${tone}" title="Ocupação ${pct(occ)}"><b>${pct(occ)}</b><i><u style="width:${width}%"></u></i></span>`;
}
function renderTrunks(rows,fresh,opctx={afterHours:false}){
  rows=normalizeInfraRows(rows);
  const visible=4;
  $("trunkInventoryMeta").textContent=inventoryMetaText(Math.min(visible,rows.length),rows.length,rows.length===1?"grupo":"grupos");
  const trunkTs=(lastData?.infra_all?.meta||{}).trunk_ts;
  $("trunkInventoryMeta").title=trunkTs?`Fotografia atual do Portal: ${String(trunkTs).replace("T"," ").slice(0,19)}`:"Fotografia atual da Instância";
  $("allTrunksBtn").textContent=rows.length?`Analisar os ${rows.length} grupos no período →`:`Analisar grupos no período →`;
  $("allTrunksBtn").disabled=!rows.length;
  if(!fresh?.usable){$("trunks").innerHTML=`<div class="source-unavailable">Infra ${fresh?.status||"indisponível"} — valores atuais não considerados</div>`;return}
  $("trunks").innerHTML=rows.slice(0,visible).map(x=>{
    const occ=finiteNumber(x.occupancy);
    const statusCls=occ===null?"unknown":occ>0?"up":"down";
    const title=occ===null?"Sem dado atual":occ>0?"Com atividade agora; pressão é indicada separadamente pela cor da ocupação":"Sem atividade agora; não implica falha por si só";
    return `<div class="data-row trunk"><span title="${esc(x.entity)}">${esc(x.entity)}</span>${occupancyMeter(occ)}<span class="channel-count"><b>${n0(x.busy)}</b> / ${n0(x.total)}</span><span class="trunk-activity ${statusCls}" title="${esc(title)}"><i class="dot-status ${statusCls}"></i></span></div>`;
  }).join("")||'<div class="data-row trunk"><span>Sem dados</span><span>—</span><span>—</span><span>N/C</span></div>'
}
function renderGateways(rows,fresh){
  rows=normalizeInfraRows(rows);
  const visible=5;
  const gatewayHead=$("gateways")?.previousElementSibling;
  if(gatewayHead?.classList.contains("gateway")){
    gatewayHead.innerHTML="<span>Gateway</span><span>Ocupação</span><span>Usados / total</span><span>Agora</span>";
  }
  $("gatewayInventoryMeta").textContent=inventoryMetaText(Math.min(visible,rows.length),rows.length,rows.length===1?"gateway":"gateways");
  const gwTs=(lastData?.infra_all?.meta||{}).gateway_ts;
  $("gatewayInventoryMeta").title=gwTs?`Fotografia atual do Portal: ${String(gwTs).replace("T"," ").slice(0,19)}`:"Fotografia atual da Instância";
  $("allGatewaysBtn").textContent=rows.length?`Ver análise dos ${rows.length} Gateways no período →`:`Ver análise dos Gateways no período →`;
  $("allGatewaysBtn").disabled=!rows.length;
  if(!fresh?.usable){$("gateways").innerHTML=`<div class="source-unavailable">Infra ${fresh?.status||"indisponível"} — valores atuais não considerados</div>`;return}
  $("gateways").innerHTML=rows.slice(0,visible).map(x=>{
    const occ=finiteNumber(x.occupancy),statusCls=occ===null?"unknown":occ>0?"up":"down";
    const title=occ===null?"Sem dado atual":occ>0?"Com tráfego agora":"Sem tráfego agora; não implica falha por si só";
    return `<div class="data-row gateway"><span title="${esc(displayGatewayName(x.entity))}">${esc(displayGatewayName(x.entity))}</span>${occupancyMeter(occ)}<span class="channel-count"><b>${n0(x.busy)}</b> / ${n0(x.total)}</span><span class="trunk-activity ${statusCls}" title="${esc(title)}"><i class="dot-status ${statusCls}"></i></span></div>`;
  }).join("")||'<div class="data-row gateway"><span>Sem dados</span><span>—</span><span>—</span><span>N/C</span></div>'
}
function renderDialer(d,status,fresh){
  if(status==="PAUSADO"||fresh?.status==="PAUSADO"){
    $("dialer").innerHTML=`<div class="source-unavailable dialer-paused">Coleta do Discador suspensa por política.<br>Última amostra: ${fresh?.age_s===null||fresh?.age_s===undefined?"—":Math.round(fresh.age_s)+"s"} atrás · não considerada no diagnóstico.</div>`;return
  }
  if(status==="SEM_CAMPANHAS"){
    $("dialer").innerHTML='<div class="source-unavailable">Fonte do Discador respondeu, porém não há campanhas retornadas para esta fila.</div>';return
  }
  if(status==="SEM_DADOS"||status==="STALE"){
    $("dialer").innerHTML=`<div class="source-unavailable">Discador ${status}: aguardando uma resposta válida da fonte background.</div>`;return
  }
  if(!["OK","LIVE","ATRASO"].includes(status))d={};
  const L=[["Total de ligações",d.calls15],["Ligações atendidas",null],["Hit Rate 5'",d.queue_hit_rate5!==null&&d.queue_hit_rate5!==undefined?pct(d.queue_hit_rate5):null],["Hit Rate 15'",d.hit15_avg!==undefined?pct(d.hit15_avg):null],["Base na tela",d.mailing_on_screen],["Disponíveis",d.mailing_available]];
  const R=[["Bloqueadas (total)",d.blocked],["Bloqueadas (horário)",d.blocked_schedule],["Bloqueadas (tentativa)",d.blocked_attempts_daily],["Finalizados CRM",d.finalized_crm],["Finalizados Tentativa",d.finalized_attempts],["Finalizados Carga",null]];
  const col=a=>`<div class="dial-col">${a.map(x=>`<div class="metric"><span>${x[0]}</span><b>${x[1]===null||x[1]===undefined?"—":typeof x[1]==="number"?n0(x[1]):x[1]}</b></div>`).join("")}</div>`;
  $("dialer").innerHTML=col(L)+col(R);
}
function renderStuck(q,s,fresh,a=null){
  const live=["LIVE","ATRASO"].includes(fresh?.status);
  if(!live) q=null;
  $("stuckCount").textContent=q===null||q===undefined?"—":q;
  $("stuckLegend").innerHTML=`
    <div class="leg"><i class="sq" style="background:#63cc6f"></i><span>Atual</span><b>${q===null||q===undefined?"—":n0(q)}</b></div>
    <div class="leg"><i class="sq" style="background:#ffb316"></i><span>Máx. ${a?periodLabel():"LIVE 15 min"}</span><b>${s?.max===null||s?.max===undefined?"—":n0(s.max)}</b></div>`;
  if(s?.episodes_count!==undefined){
    $("stuckDuration").textContent=`${n0(s.episodes_count)} episódios · ${humanMinutes(s.affected_minutes)} com travamento · ${fmt(s.channel_minutes,0)} canal-min`;
  }else{
    $("stuckDuration").textContent=`Média ${a?periodLabel():"LIVE 15 min"}: ${s?.mean===null||s?.mean===undefined?"—":fmt(s.mean,1)} canais`;
  }
}
function resetChartZoom(redraw=false){
  chartZoomState.start=0;chartZoomState.end=1;chartZoomState.dragging=false;chartZoomState.lastX=null;
  if(redraw&&lastData)renderChart(lastData,chartZoomState.lastEvents||[],lastAnalytics);
}
function chartZoomActive(){return chartZoomState.end-chartZoomState.start<.999}
function updateChartZoomStatus(){
  const z=$("chartZoomStatus"),r=$("chartZoomReset");
  if(z){
    const factor=1/Math.max(.0001,chartZoomState.end-chartZoomState.start);
    z.textContent=chartZoomActive()?`Zoom ${fmt(factor,1)}× · roda: ampliar/reduzir · arraste: mover`:`Roda do mouse: zoom · arraste: mover`;
  }
  if(r){r.style.opacity=chartZoomActive()?"1":".55";r.disabled=!chartZoomActive()}
}
function bindMainChartInteraction(){
  const c=$("mainChart");if(!c||c.dataset.zoomBound==="1")return;c.dataset.zoomBound="1";
  c.style.cursor="crosshair";
  c.addEventListener("wheel",e=>{
    if(!lastData)return;e.preventDefault();
    const rect=c.getBoundingClientRect(),plotL=40,plotR=50,plotW=Math.max(1,rect.width-plotL-plotR);
    const x=Math.max(0,Math.min(1,(e.clientX-rect.left-plotL)/plotW));
    const oldSpan=chartZoomState.end-chartZoomState.start;
    const factor=e.deltaY<0?.78:1.28;
    const newSpan=Math.max(chartZoomState.minSpan,Math.min(1,oldSpan*factor));
    const focus=chartZoomState.start+x*oldSpan;
    let start=focus-x*newSpan,end=start+newSpan;
    if(start<0){end-=start;start=0}if(end>1){start-=end-1;end=1}
    chartZoomState.start=Math.max(0,start);chartZoomState.end=Math.min(1,end);
    renderChart(lastData,chartZoomState.lastEvents||[],lastAnalytics);
  },{passive:false});
  c.addEventListener("mousedown",e=>{
    if(e.button!==0||!chartZoomActive())return;chartZoomState.dragging=true;chartZoomState.lastX=e.clientX;c.style.cursor="grabbing";e.preventDefault();
  });
  window.addEventListener("mousemove",e=>{
    if(!chartZoomState.dragging||!lastData)return;
    const rect=c.getBoundingClientRect(),plotW=Math.max(1,rect.width-90),dx=e.clientX-chartZoomState.lastX;chartZoomState.lastX=e.clientX;
    const span=chartZoomState.end-chartZoomState.start,shift=-(dx/plotW)*span;
    let start=chartZoomState.start+shift,end=chartZoomState.end+shift;
    if(start<0){end-=start;start=0}if(end>1){start-=end-1;end=1}
    chartZoomState.start=Math.max(0,start);chartZoomState.end=Math.min(1,end);
    renderChart(lastData,chartZoomState.lastEvents||[],lastAnalytics);
  });
  window.addEventListener("mouseup",()=>{if(chartZoomState.dragging){chartZoomState.dragging=false;c.style.cursor="crosshair"}});
  c.addEventListener("dblclick",()=>resetChartZoom(true));
  $("chartZoomReset")?.addEventListener("click",()=>resetChartZoom(true));
  updateChartZoomStatus();
}

function deriveEvents(series,trunks){
  const e=[];if(series.length<2)return e;
  function dominantEpisode(key,label,color){
    const vals=series.map((p,i)=>({i,v:finiteNumber(p?.[key]),ts:p?.ts})).filter(x=>x.v!==null);
    if(!vals.length)return [];
    const peak=vals.reduce((a,b)=>b.v>a.v?b:a,vals[0]);
    if(peak.v<20)return [];
    let s=peak.i;while(s>0&&finiteNumber(series[s-1]?.[key])!==null&&finiteNumber(series[s-1]?.[key])>=20)s--;
    let en=peak.i;while(en<series.length-1&&finiteNumber(series[en+1]?.[key])!==null&&finiteNumber(series[en+1]?.[key])>=20)en++;
    const out=[{ts:series[s].ts,text:`${label} crítico inicia`,val:pct(series[s][key]),color,priority:95}];
    if(peak.i!==s)out.push({ts:peak.ts,text:`${label} pico ${pct(peak.v)}`,val:pct(peak.v),color,priority:110});
    if(en<series.length-1){
      out.push({ts:series[en+1].ts,text:`${label} normaliza`,val:pct(series[en+1][key]),color:"green",priority:90});
    }else{
      const last=series.at(-1);out.push({ts:last.ts,text:`${label} crítico agora ${pct(last[key])}`,val:pct(last[key]),color,priority:105});
    }
    return out;
  }
  e.push(...dominantEpisode("p34_pct","P34","magenta"));
  e.push(...dominantEpisode("p135_pct","P135","red"));
  if(trunks.length){
    const valid=trunks.map((p,i)=>({i,v:finiteNumber(p?.occupancy),ts:p?.ts})).filter(x=>x.v!==null);
    if(valid.length){const pk=valid.reduce((a,b)=>b.v>a.v?b:a,valid[0]);if(pk.v>=85)e.push({ts:pk.ts,text:`Troncos pico ${pct(pk.v)}`,val:pct(pk.v),color:"orange",priority:100})}
  }
  const avgRate=series.reduce((s,p)=>s+(+p.dialing_rate||0),0)/series.length;
  const ir=series.findIndex(p=>(+p.dialing_rate||0)>avgRate*1.2&&(+p.dialing_rate||0)>0);
  if(ir>=0)e.push({ts:series[ir].ts,text:"Taxa de discagem ↑",val:"",color:"purple",priority:35});
  const ih=series.findIndex((p,i)=>i>0&&(+p.hit_rate15||0)<(+series[i-1].hit_rate15||0)*.78&&(+series[i-1].hit_rate15||0)>0);
  if(ih>=0)e.push({ts:series[ih].ts,text:"Hit Rate ↓",val:pct(series[ih].hit_rate15),color:"orange",priority:40});
  return e.sort((a,b)=>(b.priority||0)-(a.priority||0)||new Date(a.ts)-new Date(b.ts)).sort((a,b)=>new Date(a.ts)-new Date(b.ts));
}
function eventStamp(ts){
  if(!ts)return '—';
  const d=new Date(ts);if(Number.isNaN(d.getTime()))return '—';
  return `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
}
function eventStartCompact(start,eventTs){
  if(!start)return '—';
  const a=new Date(start),b=new Date(eventTs||start);
  if(Number.isNaN(a.getTime()))return eventStamp(start);
  const hh=`${String(a.getHours()).padStart(2,'0')}:${String(a.getMinutes()).padStart(2,'0')}`;
  if(!Number.isNaN(b.getTime())&&a.getFullYear()===b.getFullYear()&&a.getMonth()===b.getMonth()&&a.getDate()===b.getDate())return hh;
  return `${String(a.getDate()).padStart(2,'0')}/${String(a.getMonth()+1).padStart(2,'0')} ${hh}`;
}
function sameMinute(a,b){
  const x=new Date(a||0),y=new Date(b||0);
  if(Number.isNaN(x.getTime())||Number.isNaN(y.getTime()))return false;
  return Math.floor(x.getTime()/60000)===Math.floor(y.getTime()/60000);
}
function renderPeriodHighlights(d,a,events=[],opctx={afterHours:false}){
  const box=$("timeline");
  if(!box)return;
  if(!a){box.innerHTML='<div class="timeline-empty">Carregando os fatos do período selecionado…</div>';window.TOIRadar?.setPeriod(null,[]);return}
  const ei=a?.event_intelligence||{},rows=[];
  const cov=periodCoveragePct(a),cg=a?.congestion_analytics||{},po=a?.operational_pressure||{};
  const latest=a?.series?.queue?.at?.(-1)||d?.latest||{};
  const add=(x,ts=null)=>rows.push({...x,ts:ts||x.ts||null,time:x.time??eventStamp(ts||x.ts)});
  if(periodIsPartial(a))add({priority:130,time:'PERÍODO',text:`Cobertura parcial: ${fmt(cov,2)}% da fila observada`,val:'intervalos sem dados ≠ zero',color:'orange'});
  const byType=new Map((ei.highlights||[]).map(x=>[x.type,x]));
  const p34=byType.get('P34_PEAK'),p135=byType.get('P135_PEAK'),t=byType.get('TRUNK_PEAK');
  const p34Min=Number(cg?.p34?.minutes_over_20)||0,p34Ep=Number(cg?.p34?.episodes_over_20)||0;
  const p135Min=Number(cg?.p135?.minutes_over_20)||0,p135Ep=Number(cg?.p135?.episodes_over_20)||0;
  const p34Now=finiteNumber(latest?.p34_pct),p135Now=finiteNumber(latest?.p135_pct);
  const p34Life=cg?.p34?.incident_lifecycle||[],p135Life=cg?.p135?.incident_lifecycle||[];
  const activeIncident=(life,ts)=>{
    const n=Date.parse(ts||'');
    if(!Number.isFinite(n))return null;
    return [...(life||[])].reverse().find(inc=>{
      const st=Date.parse(inc?.start||'');
      const status=String(inc?.status||'').toUpperCase();
      return Number.isFinite(st)&&st<=n&&status!=='RESOLVIDO';
    })||null;
  };
  const p34Active=activeIncident(p34Life,latest.ts),p135Active=activeIncident(p135Life,latest.ts);
  if(p34Now!==null&&p34Now>=20){
    const suffix=p34Active?.start?` · desde ${eventStartCompact(p34Active.start,latest.ts)}`:'';
    add({priority:125,text:'P34 crítico agora',val:`${pct(p34Now)}${suffix}`,color:'red'},latest.ts);
  }
  if(p34&&Number(p34.value)>=20&&!(p34Now!==null&&p34Now>=20&&sameMinute(p34.ts,latest.ts)))add({priority:120,text:'P34 pico',val:pct(p34.value),color:'red'},p34.ts);
  if(p34Ep>0)add({priority:115,time:'PERÍODO',text:'P34 ≥20% no período',val:`${p34Ep} ep. · ${humanMinutes(p34Min)}`,color:'red'});
  if(p135Now!==null&&p135Now>=20){
    const suffix=p135Active?.start?` · desde ${eventStartCompact(p135Active.start,latest.ts)}`:'';
    add({priority:112,text:'P135 crítico agora',val:`${pct(p135Now)}${suffix}`,color:'red'},latest.ts);
  }
  if(p135&&Number(p135.value)>=20&&!(p135Now!==null&&p135Now>=20&&sameMinute(p135.ts,latest.ts)))add({priority:108,text:'P135 pico',val:pct(p135.value),color:'red'},p135.ts);
  if(p135Ep>0)add({priority:104,time:'PERÍODO',text:'P135 ≥20% no período',val:`${p135Ep} ep. · ${humanMinutes(p135Min)}`,color:'red'});
  if(t)add({priority:Number(t.value)>=90?110:90,text:`Troncos pico${t.entity?` · ${t.entity}`:''}`,val:pct(t.value),color:t.value>=90?'red':t.value>=80?'orange':'green'},t.ts);
  const sc=byType.get('STUCK_PEAK'),stuckState=String(po?.dimensions?.stuck_state||a?.capacity_analytics?.operational?.dimensions?.stuck_channels?.state||'').toUpperCase();
  if(sc&&['AGRAVANTE','PRESENTE'].includes(stuckState))add({priority:70,text:`Canais travados · ${stuckState.toLowerCase()}`,val:n0(sc.value),color:stuckState==='AGRAVANTE'?'red':'orange'},sc.ts);

  // Lifecycle de P34/P135 no PERÍODO. Resolução exige primeira amostra <20% sem gap.
  // Estes fatos são a mesma verdade temporal usada por Incidentes Recentes.
  const boundsStart=Date.parse(a?.bounds?.start||''),boundsEnd=Date.parse(a?.bounds?.end||'');
  const inBounds=ts=>{const n=Date.parse(ts||'');return Number.isFinite(n)&&(!Number.isFinite(boundsStart)||n>=boundsStart)&&(!Number.isFinite(boundsEnd)||n<=boundsEnd)};
  for(const [metric,label,color,lifecycle,nowVal] of [['p34','P34','magenta',p34Life,p34Now],['p135','P135','red',p135Life,p135Now]]){
    for(const inc of lifecycle){
      const resolved=String(inc?.status||'').toUpperCase()==='RESOLVIDO';
      const mergedIntoCurrent=!resolved&&nowVal!==null&&nowVal>=20&&sameMinute(inc?.start,latest.ts);
      if(inBounds(inc?.start)&&!mergedIntoCurrent)add({priority:94,text:`${label} crítico iniciou`,val:`${finiteNumber(inc.max)!==null?`pico ${pct(inc.max)}`:'limiar ≥20%'}`,color},inc.start);
      if(resolved&&inc?.resolved_at&&inBounds(inc.resolved_at)){
        add({priority:102,text:`${label} normalizado`,val:`pico ${pct(inc.max)} · iniciou ${eventStartCompact(inc.start,inc.resolved_at)}`,color:'green'},inc.resolved_at);
      }
    }
  }

  // Gateway sem ocupação AGORA: fato observacional do período, não diagnóstico de falha.
  const ca=a?.capacity_analytics||{},capOp=ca?.operational||{};
  const gwProfiles=(capOp?.gateway_profiles||ca?.gateway_profiles||ca?.instance_analytics?.gateway_profiles||[]);
  const gwCtx=(capOp?.gateway_contexts||ca?.gateway_contexts||[]);
  const gwSem=new Map(gwCtx.map(x=>[String(x?.entity||'').toUpperCase(),String(x?.canonical_status||'').toUpperCase()]));
  const zeroGw=gwProfiles.filter(g=>{
    const cur=finiteNumber(g?.current_pct),dur=finiteNumber(g?.current_zero_duration_min);
    const sem=gwSem.get(String(g?.entity||'').toUpperCase())||'';
    return cur!==null&&cur<=0&&dur!==null&&dur>=1&&!['PLANNED_INACTIVE','STANDBY_EXPECTED'].includes(sem);
  }).sort((x,y)=>(finiteNumber(y?.current_zero_duration_min)||0)-(finiteNumber(x?.current_zero_duration_min)||0));
  for(const g of zeroGw){
    const dur=finiteNumber(g?.current_zero_duration_min)||0,left=!!g?.current_zero_left_censored;
    const priority=dur>=45?106:dur>=20?98:dur>=10?88:76;
    const color=dur>=45?'red':dur>=20?'orange':'purple';
    const entity=String(g?.entity||'N/C').toUpperCase();
    add({priority,text:`GW ${entity} sem ocupação agora`,val:`${left?'≥ ':''}${humanMinutes(dur)} · desde ${eventStartCompact(g?.current_zero_since,g?.current_zero_since)}`,color,fullText:`Gateway ${entity} sem ocupação observada agora`},g?.current_zero_since);
  }

  // H3: preserve completed zero-occupancy lifecycle after the Gateway resumes activity.
  const recoveredGw=gwProfiles.filter(g=>{
    const dur=finiteNumber(g?.last_zero_episode_duration_min);
    const recovery=g?.last_zero_recovery_at;
    const sem=gwSem.get(String(g?.entity||'').toUpperCase())||'';
    return !!recovery&&dur!==null&&dur>=1&&!['PLANNED_INACTIVE','STANDBY_EXPECTED'].includes(sem);
  }).sort((x,y)=>new Date(y?.last_zero_recovery_at||0)-new Date(x?.last_zero_recovery_at||0));
  for(const g of recoveredGw){
    const entity=String(g?.entity||'N/C').toUpperCase();
    const dur=finiteNumber(g?.last_zero_episode_duration_min)||0,left=!!g?.last_zero_episode_left_censored;
    const recPct=finiteNumber(g?.last_zero_recovery_pct);
    const basePriority=dur>=45?104:dur>=20?96:dur>=10?86:74;
    const startColor=dur>=45?'red':dur>=20?'orange':'purple';
    add({priority:basePriority,text:`GW ${entity} sem ocupação`,val:`${left?'≥ ':''}${humanMinutes(dur)} · retomou ${eventStartCompact(g?.last_zero_recovery_at,g?.last_zero_recovery_at)}`,color:startColor,fullText:`Gateway ficou sem ocupação observada · ${entity}`},g?.last_zero_episode_since);
    add({priority:basePriority+3,text:`GW ${entity} voltou a ocupar`,val:`${recPct!==null?pct(recPct)+' · ':''}após ${left?'≥ ':''}${humanMinutes(dur)}`,color:'green',fullText:`Gateway voltou a ocupar · ${entity}`},g?.last_zero_recovery_at);
  }

  const dc=byType.get('DIALING_PEAK');if(dc)add({priority:60,text:'Discando · pico simultâneo',val:n0(dc.value),color:'purple'},dc.ts);
  for(const e of (events||[])){
    const txt=String(e.text||'Evento observado');
    if(/\bpico\b/i.test(txt)||/crítico agora/i.test(txt)||/^(P34|P135) (crítico inicia|normaliza)/i.test(txt))continue;
    add({priority:Number(e.priority)||50,text:txt,val:e.val||'',color:e.color||'purple'},e.ts);
  }
  const unique=[];const seen=new Set();
  for(const x of rows){const k=`${x.ts||x.time}|${x.text}|${x.val}`;if(!seen.has(k)){seen.add(k);unique.push(x)}}
  // Regra executiva: fatos datados SEMPRE em cronologia reversa (mais recentes no topo).
  // Resumos sem instante único pertencem ao período e ficam após os fatos cronológicos.
  unique.sort((a,b)=>{
    const at=Date.parse(a.ts||''),bt=Date.parse(b.ts||''),ah=Number.isFinite(at),bh=Number.isFinite(bt);
    if(ah!==bh)return ah?-1:1;
    if(ah&&bt!==at)return bt-at;
    return (b.priority||0)-(a.priority||0)||String(a.text||'').localeCompare(String(b.text||''));
  });
  if(window.TOIRadar){
    window.TOIRadar.setPeriod({start:a?.bounds?.start,end:a?.bounds?.end,
      instancia:$("instanceSelect").value,codigo:$("queueSelect").value,label:periodLabel()},unique);
    return;
  }
  box.style.overflowY=unique.length>4?'auto':'visible';box.style.paddingRight=unique.length>4?'3px':'';
  box.innerHTML=unique.map((x,i)=>{
    const title=[x.fullText||x.text,x.val].filter(Boolean).join(' · ');
    return `<div class="timeline-item" title="${esc(title)}"><span class="timeline-time">${x.time}</span><i class="event-dot ${x.color}">${i+1}</i><b>${x.text}</b><span class="timeline-value">${x.val}</span></div>`;
  }).join('')||'<div class="timeline-empty">Sem evidência suficiente para destacar eventos no período selecionado.</div>';
}
function renderChart(d,events=[],a=lastAnalytics){
  const profileMode=a?.mode==="PROFILE_24H";
  let series=[],infra=[],stuckSeries=[];

  if(profileMode){
    const p=a?.profile||[];
    series=p.map(x=>({
      __x:x.minute_of_day,
      p135_pct:finiteNumber(x.p135_pct?.mean),
      p34_pct:finiteNumber(x.p34_pct?.mean),
      hit_rate15:finiteNumber(x.hit_rate15?.mean),
      calls_dialing:finiteNumber(x.calls_dialing?.mean),
      calls_in_attendance:finiteNumber(x.calls_in_attendance?.mean)
    }));
    infra=p.map(x=>({__x:x.minute_of_day,occupancy:finiteNumber(x.trunk_occupancy?.p95)}));
    stuckSeries=p.map(x=>({__x:x.minute_of_day,quantity:finiteNumber(x.stuck_quantity?.mean)}));
  }else{
    series=a?.series?.queue||d.series||[];
    infra=a?.series?.trunk||d.infra_series?.trunk||[];
    stuckSeries=a?.series?.stuck||d.stuck_series||[];
  }

  const c=$("mainChart"),r=c.getBoundingClientRect(),dp=window.devicePixelRatio||1,W=r.width,H=r.height;
  c.width=W*dp;c.height=H*dp;
  const ctx=c.getContext("2d");
  ctx.setTransform(dp,0,0,dp,0,0);
  ctx.clearRect(0,0,W,H);
  ctx.lineCap="round";ctx.lineJoin="round";

  const p={l:40,r:50,t:22,b:32},PW=W-p.l-p.r,PH=H-p.t-p.b;
  ctx.font='9px "Segoe UI"';ctx.strokeStyle="#17394c";ctx.fillStyle="#8296a4";ctx.lineWidth=1;
  for(let i=0;i<=4;i++){
    const yy=p.t+PH*i/4;
    ctx.beginPath();ctx.moveTo(p.l,yy);ctx.lineTo(W-p.r,yy);ctx.stroke();
    ctx.fillText(`${100-i*25}%`,2,yy+3);
  }

  const yPct=v=>p.t+PH*(1-Math.max(0,Math.min(100,Number(v)||0))/100);
  const y10=yPct(10),y20=yPct(20);
  ctx.save();ctx.setLineDash([4,4]);ctx.lineWidth=.8;
  ctx.strokeStyle="#9a7d2a";ctx.beginPath();ctx.moveTo(p.l,y10);ctx.lineTo(W-p.r,y10);ctx.stroke();
  ctx.strokeStyle="#8b3439";ctx.beginPath();ctx.moveTo(p.l,y20);ctx.lineTo(W-p.r,y20);ctx.stroke();ctx.restore();
  ctx.font='8px "Segoe UI"';ctx.fillStyle="#c6a13b";ctx.fillText("atenção 10%",p.l+4,y10-3);
  ctx.fillStyle="#d65a60";ctx.fillText("crítico 20%",p.l+4,y20-3);

  const validSeries=series.filter(x=>profileMode?x.__x!==undefined:!!x.ts);
  if(!validSeries.length){
    ctx.fillStyle="#78909d";ctx.font='10px "Segoe UI"';
    ctx.fillText("Sem amostras suficientes para esta janela.",p.l+12,p.t+24);
    return;
  }

  let fullT0,fullT1,fullSpan,t0,t1,span,xx;
  if(profileMode){fullT0=0;fullT1=1439;fullSpan=1439}else{
    fullT0=new Date(validSeries[0].ts).getTime();fullT1=new Date(validSeries.at(-1).ts).getTime();fullSpan=Math.max(1,fullT1-fullT0);
  }
  const zoomKey=`${analyticsSelectionKey($("instanceSelect")?.value||"",$("queueSelect")?.value||"")}|${profileMode?'profile':'chrono'}`;
  if(chartZoomState.key!==zoomKey){chartZoomState.key=zoomKey;chartZoomState.start=0;chartZoomState.end=1}
  let step=profileMode?15:null;
  if(!profileMode){
    const stamps=validSeries.slice(0,300).map(x=>new Date(x.ts).getTime()).filter(Number.isFinite);const diffs=[];
    for(let i=1;i<stamps.length;i++){const d=stamps[i]-stamps[i-1];if(d>0)diffs.push(d)}
    if(diffs.length){diffs.sort((a,b)=>a-b);step=diffs[Math.floor(diffs.length/2)]}
  }
  chartZoomState.minSpan=Math.min(1,Math.max(.0005,((step||fullSpan*.01)*12)/Math.max(1,fullSpan)));
  const zs=Math.max(0,Math.min(1-chartZoomState.minSpan,chartZoomState.start));
  const ze=Math.max(zs+chartZoomState.minSpan,Math.min(1,chartZoomState.end));chartZoomState.start=zs;chartZoomState.end=ze;
  t0=fullT0+fullSpan*zs;t1=fullT0+fullSpan*ze;span=Math.max(1,t1-t0);
  xx=profileMode
    ?v=>p.l+PW*((Number(v)||0)-t0)/span
    :v=>p.l+PW*(new Date(v).getTime()-t0)/span;
  chartZoomState.profileMode=profileMode;chartZoomState.lastEvents=events||[];updateChartZoomStatus();
  const yp=v=>p.t+PH*(1-Math.max(0,Math.min(100,Number(v)||0))/100);

  const callVals=series.flatMap(q=>[finiteNumber(q.calls_dialing),finiteNumber(q.calls_in_attendance)]).filter(v=>v!==null);
  const maxObserved=Math.max(...callVals,1);
  const vmax=Math.max(1500,Math.ceil(maxObserved/500)*500);
  const yv=v=>p.t+PH*(1-Math.max(0,Number(v)||0)/vmax);

  ctx.fillStyle="#74d21d";ctx.font='9px "Segoe UI"';
  for(let i=0;i<=3;i++){
    const value=Math.round(vmax*(3-i)/3),yy=p.t+PH*i/3;
    ctx.fillText(value.toLocaleString("pt-BR"),W-p.r+8,yy+3);
  }

  ctx.fillStyle="#8296a4";
  if(profileMode){
    // H2-B: leitura executiva hora a hora na janela operacional, sobretudo 08:00–21:00.
    const ticks=[0,240]; for(let h=8;h<=21;h++)ticks.push(h*60); ticks.push(1439);
    const unique=[...new Set(ticks)].sort((a,b)=>a-b);
    ctx.save();ctx.font='7.4px "Segoe UI"';
    unique.forEach(m=>{
      const px=xx(m),label=m===1439?"23:59":`${String(Math.floor(m/60)).padStart(2,"0")}:00`;
      if(m>=480&&m<=1260){ctx.strokeStyle="#102f40";ctx.lineWidth=.7;ctx.beginPath();ctx.moveTo(px,p.t);ctx.lineTo(px,p.t+PH);ctx.stroke()}
      ctx.fillStyle=(m>=480&&m<=1260)?"#9aadb7":"#708793";
      ctx.fillText(label,Math.max(p.l,Math.min(W-p.r-24,px-11)),H-18);
    });
    ctx.restore();
  }else{
    const longChronology=span>24*3600*1000;
    for(let i=0;i<=7;i++){
      const tt=t0+span*i/7,px=p.l+PW*i/7,dt=new Date(tt);
      const label=longChronology
        ?`${String(dt.getDate()).padStart(2,"0")}/${String(dt.getMonth()+1).padStart(2,"0")} ${dt.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})}`
        :dt.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"});
      ctx.fillText(label,Math.max(p.l,Math.min(W-p.r-48,px-(longChronology?25:14))),H-18);
    }
  }

  function rowX(q){return profileMode?xx(q.__x):xx(q.ts)}
  function rowT(q){return profileMode?Number(q.__x):new Date(q.ts).getTime()}
  function drawSegmented(arr,key,yFn,color,width,gapThresholdMs=null){
    ctx.strokeStyle=color;ctx.lineWidth=width;
    let started=false,prevT=null;
    ctx.beginPath();
    for(const q of arr){
      const v=finiteNumber(q?.[key]),x=rowX(q),t=rowT(q);
      if(v===null||!Number.isFinite(x)||!Number.isFinite(t)||t<t0||t>t1){
        started=false;prevT=null;continue;
      }
      const y=yFn(v);
      const gap=prevT===null?0:t-prevT;
      const gapBreak=profileMode?(prevT!==null&&gap>ANALYTICS_PROFILE_GAP_MIN):(gapThresholdMs&&prevT!==null&&gap>gapThresholdMs);
      if(!started||gapBreak){ctx.moveTo(x,y);started=true}else ctx.lineTo(x,y);
      prevT=t;
    }
    ctx.stroke();
  }

  const ANALYTICS_PROFILE_GAP_MIN=15*1.5;
  const qGap=(finiteNumber(a?.coverage?.queue?.gap_threshold_s)||90)*1000;
  const tGap=(finiteNumber(a?.coverage?.trunk?.gap_threshold_s)||180)*1000;
  const sGap=(finiteNumber(a?.coverage?.stuck?.gap_threshold_s)||180)*1000;

  drawSegmented(series,"p135_pct",yp,"#ff3d45",1.55,qGap);
  drawSegmented(series,"p34_pct",yp,"#ff4fd8",1.25,qGap);
  drawSegmented(series,"hit_rate15",yp,"#ff9818",1.25,qGap);
  drawSegmented(series,"calls_dialing",yv,"#179fe3",1.35,qGap);
  drawSegmented(series,"calls_in_attendance",yv,"#25a9bf",1.10,qGap);
  if(infra.length)drawSegmented(infra,"occupancy",yp,"#74d21d",1.35,tGap);

  const stuckVals=stuckSeries.map(q=>finiteNumber(q.quantity)).filter(v=>v!==null);
  if(stuckVals.length){
    const stuckMax=Math.max(...stuckVals,1),stuckBand=PH*0.12;
    const yStuck=v=>p.t+PH-(Math.max(0,Number(v)||0)/stuckMax)*stuckBand;
    drawSegmented(stuckSeries,"quantity",yStuck,"#ffb316",1.45,sGap);
    ctx.fillStyle="#ffb316";ctx.font='8px "Segoe UI"';
    ctx.fillText(`${fmt(stuckMax,0)} canais`,W-p.r+7,p.t+PH-stuckBand+3);
    ctx.fillText("0",W-p.r+7,p.t+PH+3);
  }

  // Eventos pertencem à cronologia real. Em Perfil Horário não misturamos
  // um timestamp absoluto com a distribuição por hora do dia.
  if(!profileMode){
    const visible=(events||[]).map(e=>({...e,__px:xx(e.ts)})).filter(e=>Number.isFinite(e.__px)&&e.__px>=p.l&&e.__px<=W-p.r).sort((a,b)=>a.__px-b.__px);
    const bw=148,bh=34,laneGap=4,laneEnds=[-Infinity,-Infinity,-Infinity,-Infinity];
    visible.forEach(e=>{
      const px=e.__px,bx=Math.max(p.l,Math.min(W-p.r-bw,px-bw/2));
      let lane=laneEnds.findIndex(end=>bx>end+laneGap);if(lane<0)lane=laneEnds.indexOf(Math.min(...laneEnds));
      laneEnds[lane]=bx+bw;const by=p.t+2+lane*(bh+3);
      ctx.save();ctx.setLineDash([3,3]);ctx.strokeStyle="#7453c7";ctx.lineWidth=1;
      ctx.beginPath();ctx.moveTo(px,Math.min(p.t+PH,by+bh));ctx.lineTo(px,p.t+PH);ctx.stroke();ctx.restore();
      ctx.fillStyle="#171b35";ctx.strokeStyle="#6d4bc1";ctx.lineWidth=1;ctx.beginPath();
      if(ctx.roundRect)ctx.roundRect(bx,by,bw,bh,3);else ctx.rect(bx,by,bw,bh);
      ctx.fill();ctx.stroke();ctx.fillStyle="#ac8cff";ctx.font='9px "Segoe UI"';
      ctx.fillText(eventStamp(e.ts).slice(-5),bx+7,by+12);ctx.fillStyle="#c9d2d8";ctx.font='8.5px "Segoe UI"';
      ctx.fillText(String(e.text||'').slice(0,31),bx+7,by+27);
    });
  }
}

function mlTone(riskClass,score){
  const c=String(riskClass||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"").toUpperCase();
  if(c==="ALTO"||Number(score)>=70)return {cls:"alto",color:"#ff3d45"};
  if(c==="MEDIO"||Number(score)>=40)return {cls:"medio",color:"#ffb316"};
  if(c==="BAIXO"||Number.isFinite(Number(score)))return {cls:"baixo",color:"#65d27a"};
  return {cls:"neutral",color:"#8da0ad"};
}
function mlAgeSeconds(ts){
  if(!ts)return null;
  const t=new Date(ts).getTime();
  if(!Number.isFinite(t))return null;
  return Math.max(0,(Date.now()-t)/1000);
}
function mlTime(ts){
  if(!ts)return "—";
  const d=new Date(ts);
  if(Number.isNaN(d.getTime()))return String(ts).slice(0,19).replace("T"," ");
  return d.toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit",second:"2-digit"});
}
function mlAgeLabel(age){
  if(age===null||age===undefined)return "—";
  if(age<60)return `${Math.round(age)}s`;
  if(age<3600)return `${Math.floor(age/60)}min ${Math.round(age%60)}s`;
  return `${Math.floor(age/3600)}h ${Math.floor((age%3600)/60)}min`;
}
function finiteNumber(v){
  if(v===null||v===undefined||v==="")return null;
  const n=Number(v);return Number.isFinite(n)?n:null;
}
function mlSignalDefinitions(ml,queueUsable,infraUsable){
  const rz=ml?.features?.robust_z||{};
  const adverseHit=finiteNumber(rz.hit_rate15); 
  const defs=[
    {key:"p135",label:"P135",value:queueUsable?finiteNumber(ml?.p135_signal):null,source:"queue"},
    {key:"p34",label:"P34",value:queueUsable?finiteNumber(ml?.p34_signal):null,source:"queue"},
    {key:"hit",label:"Hit Rate 15 ↓",value:queueUsable&&adverseHit!==null?Math.max(0,-adverseHit):null,source:"queue"},
    {key:"calls",label:"Discando ↑",value:queueUsable?Math.max(0,finiteNumber(rz.calls_dialing)||0):null,source:"queue"},
    {key:"rate",label:"Taxa discagem ↑",value:queueUsable?Math.max(0,finiteNumber(rz.dialing_rate)||0):null,source:"queue"},
    {key:"capacity",label:"Pressão troncos",value:infraUsable?finiteNumber(ml?.capacity_signal):null,source:"infra"},
  ];
  return defs;
}
function dominantMlSignal(defs){
  const valid=defs.filter(x=>x.value!==null&&x.value!==undefined&&Number.isFinite(Number(x.value)));
  if(!valid.length)return "—";
  const top=valid.reduce((a,b)=>Number(b.value)>Number(a.value)?b:a);
  if(Number(top.value)<1)return "Sem sinal dominante";
  return `${top.label} (${fmt(top.value,2)})`;
}
function renderMlShadow(d,opctx=operationalContextFor(d)){
  const ml=d?.ml_shadow||{};
  const src=d?.freshness?.sources||{};
  const queueStatus=src?.queue?.status||"SEM_DADOS";
  const infraStatus=src?.infra?.status||"SEM_DADOS";
  const queueUsable=src?.queue?.usable===true||["LIVE","ATRASO"].includes(queueStatus);
  const infraUsable=src?.infra?.usable===true||["LIVE","ATRASO"].includes(infraStatus);
  const score=finiteNumber(ml?.risk_score);
  const hasPrediction=score!==null;
  const age=mlAgeSeconds(ml?.ts);
  const inferenceFresh=age!==null&&age<=120;
  const inputsValid=queueUsable&&infraUsable;
  const valid=hasPrediction&&inputsValid&&inferenceFresh;
  const tone=mlTone(ml?.risk_class,valid?score:null);
  const riskClass=valid?(ml?.risk_class||"BAIXO"):(hasPrediction?"SUPRIMIDO":"AGUARDANDO");
  const reasons=[];
  if(!queueUsable)reasons.push(`Fila ${queueStatus}`);
  if(!infraUsable)reasons.push(`Infra ${infraStatus}`);
  if(hasPrediction&&age===null)reasons.push('horário da inferência ausente ou inválido');
  if(hasPrediction&&age!==null&&!inferenceFresh)reasons.push(`inferência ${mlAgeLabel(age)} atrás`);

  const ring=$("mlRiskRing");
  if(ring){
    ring.style.setProperty("--ml-risk",valid?Math.max(0,Math.min(100,score)):0);
    ring.style.setProperty("--ml-tone",tone.color);
    ring.classList.toggle("suppressed",hasPrediction&&!valid);
  }
  $("mlRiskScore").textContent=valid?fmt(score,1):"—";
  $("mlRiskClass").textContent=riskClass;
  $("mlRiskClass").className=`ml-risk-class ${valid?tone.cls:(hasPrediction?"suppressed":"neutral")}`;

  const gate=$("mlGateBadge");
  if(valid){gate.textContent="INPUTS VÁLIDOS";gate.className="ml-gate-badge ok"}
  else if(hasPrediction){gate.textContent="QUALITY GATE";gate.className="ml-gate-badge blocked"}
  else{gate.textContent="AGUARDANDO";gate.className="ml-gate-badge neutral"}

  const ctxBadge=$("mlContextBadge");
  if(ctxBadge){
    ctxBadge.textContent=opctx.afterHours?"OBSERVAÇÃO PASSIVA":opctx.configured?"JANELA OPERACIONAL":"CONTEXTO N/C";
    ctxBadge.className=`ml-context-badge ${opctx.afterHours?"outside":opctx.configured?"inside":"neutral"}`;
  }
  const model=ml?.model_version||ml?.model_status||"—";
  $("mlModelVersion").textContent=model;
  $("mlInferenceTime").textContent=mlTime(ml?.ts);
  $("mlInferenceAge").textContent=age===null?"—":mlAgeLabel(age);
  $("mlInferenceAge").className=(age!==null&&age>120)?"ml-text-bad":"";

  const f=ml?.features||{};
  const samples=finiteNumber(f?.samples);
  const hours=finiteNumber(f?.history_hours);
  $("mlBaseline").textContent=samples!==null?`${hours!==null?fmt(hours,0)+"h · ":""}${n0(samples)} amostras`:"aguardando baseline";
  $("mlInputQuality").textContent=`Fila ${queueStatus} · Infra ${infraStatus}`;
  $("mlInputQuality").className=inputsValid?"ml-text-ok":"ml-text-bad";

  let explanation;
  if(valid)explanation=ml?.explanation||"Sem anomalia dominante no baseline robusto";
  else if(hasPrediction)explanation=`Inferência registrada, porém suprimida nesta tela: ${reasons.join(" · ")||"entradas não validadas"}.`;
  else explanation=ml?.note||"Aguardando baseline mínimo e primeira inferência persistida.";
  $("mlExplanation").textContent=explanation;
  const relevant=$('mlRelevantInsight'),reportLive=$('mlReportLive');
  if(relevant){
    const signal=valid?dominantMlSignal(mlSignalDefinitions(ml,queueUsable,infraUsable)):null;
    const riskLevel=String(ml?.risk_class||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toUpperCase();
    const meaningful=valid&&(riskLevel==='ALTO'||riskLevel==='MEDIO');
    let message=meaningful
      ?`Sinal relevante às ${mlTime(ml?.ts)}: ${signal}. ${explanation}`
      :valid?`Sem sinal relevante nesta leitura (${mlTime(ml?.ts)}). O Shadow segue acompanhando a fila.`
        :`Leitura não disponível: ${explanation}`;
    const memoryKey=`toi.ml.relevant.${$('instanceSelect')?.value||''}.${$('queueSelect')?.value||''}`;
    try{
      if(meaningful)localStorage.setItem(memoryKey,JSON.stringify({ts:ml.ts,message}));
      else{
        const previous=JSON.parse(localStorage.getItem(memoryKey)||'null');
        if(previous?.ts&&Date.now()-Date.parse(previous.ts)<86400000&&Date.now()-Date.parse(previous.ts)>=0)
          message=`Último ${previous.message} Leitura atual: ${valid?'sem novo sinal relevante':'indisponível'}.`;
      }
    }catch(_error){/* O armazenamento local é opcional; a leitura atual continua visível. */}
    relevant.textContent=message;
    if(reportLive)reportLive.textContent=message;
    relevant.classList.toggle('notable',meaningful);
  }

  const defs=mlSignalDefinitions(ml,queueUsable,infraUsable);
  $("mlDominantSignal").textContent=valid?dominantMlSignal(defs):"—";
  $("mlSignals").innerHTML=defs.map(x=>{
    const v=x.value;
    const usable=v!==null&&v!==undefined&&Number.isFinite(Number(v));
    const width=usable?Math.max(0,Math.min(100,Number(v)/4*100)):0;
    const level=!usable?"excluded":Number(v)>=2?"strong":Number(v)>=1?"watch":"low";
    return `<div class="ml-signal ${level}"><div><span>${x.label}</span><b>${usable?fmt(v,2):"EXCLUÍDO"}</b></div><i><u style="width:${width}%"></u></i></div>`;
  }).join("");

  const qscore=finiteNumber(d?.quality_gate?.score);
  const exclusions=(d?.quality_gate?.excluded_sources||[]).filter(Boolean);
  const contextNote=opctx.afterHours?` Fora da janela operacional ${opctx.windowText||""}: score preservado para observabilidade, não interpretado como saúde sob carga.`:"";
  const governance=valid
    ?`Somente observação. Quality Gate ${qscore===null?"—":fmt(qscore,2)+"%"}. Não influencia diagnóstico, recomendação, chamado ou ação.${contextNote}`
    :hasPrediction
      ?`Saída Shadow preservada para auditoria, mas não apresentada como válida. ${reasons.join(" · ")||"Quality Gate bloqueou a leitura"}.`
      :"Somente observação. Nenhuma decisão é produzida sem uma inferência válida e auditável.";
  $("mlGovernanceNote").textContent=governance+(exclusions.length?` Fontes excluídas no ciclo: ${exclusions.join(", ")}.`:"");

  const head=$("mlExecutiveSummary");
  if(head){
    if(valid){
      head.textContent=opctx.afterHours
        ?`ML PASSIVO · ${fmt(score,1)}/100 ${ml?.risk_class||"—"} · ${age===null?"atual":mlAgeLabel(age)}`
        :`ML SHADOW · ${fmt(score,1)}/100 ${ml?.risk_class||"—"} · ${age===null?"atual":mlAgeLabel(age)}`;
      head.className=`ml-exec-summary ${opctx.afterHours?"outside":tone.cls}`;
    }else if(hasPrediction){
      head.textContent=`ML SHADOW · SUPRIMIDO · ${reasons.join(" · ")||"Quality Gate"}`;
      head.className="ml-exec-summary blocked";
    }else{
      head.textContent="ML SHADOW · aguardando baseline/inferência";
      head.className="ml-exec-summary neutral";
    }
  }
}

function renderRecs(r,opctx={afterHours:false}){
  let a=[...r].slice(0,3);
  if(opctx.afterHours){
    a=[{title:"Nenhuma intervenção operacional fora da janela",why:`Fora da janela operacional ${opctx.windowText||""}; zeros de agentes/discagem não são tratados como pressão operacional.`,expected:"Manter coleta histórica e observabilidade 24x7.",priority:"Baixa",contextual:true}];
  }
  $("recCount").textContent=opctx.afterHours?"fora da janela":`${a.length} recomendações ativas`;
  const grid=$("recommendations");grid.dataset.count=String(Math.max(1,a.length));
  grid.innerHTML=a.map((q,i)=>`<div class="rec ${q.contextual?"contextual":""}"><div><span class="rec-num">${i+1}</span><span class="rec-title">${q.title}</span></div><p>${q.why}</p><p class="impact">Impacto esperado: ${q.expected}</p><span class="priority ${q.priority}">${q.priority==="—"?"Sem prioridade":"Prioridade: "+q.priority}</span></div>`).join("")||'<div class="rec placeholder">Nenhuma recomendação nesta janela.</div>';
}
function renderCapacity(c,d={},opctx={afterHours:false},a=lastAnalytics){
  const ca=a?.capacity_analytics||{},op=ca?.operational||c||{};
  const p95=finiteNumber(op.p95_occupancy),headroom=finiteNumber(op.headroom);
  const telecomBasis=op.telecom_risk_basis||{},telecomUse=finiteNumber(telecomBasis.occupancy_pct);
  const hist=op.historical_pressure_group||{},critical=op.critical_group||{};
  const status=op.pressure_status||op.overall_status||(p95===null?"N/C":p95>=90?"ALTO":p95>=80?"ATENÇÃO":"BAIXO");
  const riskClass=status==="ALTO"?"alto":status==="ATENÇÃO"?"médio":status==="BAIXO"?"baixo":"neutral";
  const gwNow=finiteNumber(op.gateway_current_max),gwNowEntity=op.gateway_current_max_entity||"—";
  const gwP95=finiteNumber(op.gateway_p95),gwP95Entity=op.gateway_p95_entity||"—";
  const currentGroup=critical.entity||"—",periodGroup=hist.entity||critical.entity||"—";
  const infraFresh=d?.freshness?.sources?.infra?.usable===true||
    ["LIVE","ATRASO"].includes(d?.freshness?.sources?.infra?.status);
  const gateways=infraFresh?normalizeInfraRows((d?.infra_all||d?.infra||{}).gateways)
    .filter(x=>finiteNumber(x.occupancy)!==null).sort((x,y)=>Number(y.occupancy)-Number(x.occupancy)):[];
  const most=gateways[0],least=gateways.length>1?gateways[gateways.length-1]:null;
  const ml=d?.ml_shadow||{},mlScore=finiteNumber(ml.risk_score),mlAge=mlAgeSeconds(ml.ts);
  const mlValid=mlScore!==null&&mlAge!==null&&mlAge<=120&&
    ["LIVE","ATRASO"].includes(d?.freshness?.sources?.queue?.status)&&infraFresh;
  const mlReason=mlAge===null?'horário da inferência ausente':mlAge>120?`inferência de ${mlAgeLabel(mlAge)} atrás`:
    !["LIVE","ATRASO"].includes(d?.freshness?.sources?.queue?.status)?`fila ${d?.freshness?.sources?.queue?.status||'sem dados'}`:
    !infraFresh?`infraestrutura ${d?.freshness?.sources?.infra?.status||'sem dados'}`:'entradas sem validação';
  const mlValue=mlValid?`${fmt(mlScore,2)}% · ${esc(ml.risk_class||'BAIXO')}`:
    mlScore!==null?`Suprimido · ${esc(mlReason)}`:'Aguardando';
  const gwMetric=x=>x?`${pct(x.occupancy)} · ${esc(displayGatewayName(x.entity))}`:'N/C';
  $("pressureCapacitySummary").innerHTML=`
    <div class="pressure-metric"><span>Capacidade</span><b>${esc(status)}</b></div>
    <div class="pressure-metric" title="${esc(telecomBasis.method||'Maior ocupação sustentada válida no caminho Telecom')}"><span>Pressão Telecom no período · ${esc(telecomBasis.source==='GATEWAY'?'Gateway':telecomBasis.source==='TRUNK'?'troncos':'N/C')}</span><b>${pct(telecomUse)}</b></div>
    <div class="pressure-metric"><span>Folga no nível que 95% das leituras não ultrapassaram</span><b>${pct(headroom)}</b></div>
    <div class="pressure-metric" title="Gateway de maior ocupação na fotografia atual"><span>Gateway mais ocupado</span><b>${gwMetric(most)}</b></div>
    <div class="pressure-metric" title="Gateway de menor ocupação na fotografia atual; zero pode ser standby"><span>Gateway menos ocupado</span><b>${gwMetric(least)}</b></div>
    <div class="pressure-metric" tabindex="0" title="${esc(mlValid?'Inferência atual com entradas válidas':mlReason)}. O ML Shadow observa a fila e não dispara alertas nem ações."><span>ML Shadow</span><b>${mlValue}</b></div>`;
  $("capacity").innerHTML=`
    <div class="capacity-grid capacity-grid-v2">
      <div class="capacity-risk">
        <div class="risk-caption">Pressão de capacidade · ${esc(a?.period_label||periodLabel())}</div>
        <div class="risk-value risk-${riskClass}">${esc(status)}</div>
        <div class="capacity-headroom-label">Folga no nível que 95% das leituras não ultrapassaram · ${esc(periodGroup)}</div>
        <div class="capacity-headroom">${pct(headroom)}</div>
      </div>
      <div class="capacity-metrics">
        <div class="capacity-metric capacity-bottleneck"><span>No período · maior valor que 95% das leituras não ultrapassaram</span><b title="${esc(periodGroup)}">${pct(p95)} · ${esc(periodGroup)}</b></div>
        <div class="capacity-metric"><span>Agora · grupo mais ocupado</span><b>${pct(critical.occupancy)} · ${esc(currentGroup)}</b></div>
        <div class="capacity-metric"><span>Agora · Gateway mais ocupado</span><b>${pct(gwNow)} · ${esc(displayGatewayName(gwNowEntity))}</b></div>
        <div class="capacity-metric"><span>No período · Gateway com maior valor não ultrapassado em 95% das leituras</span><b>${pct(gwP95)} · ${esc(displayGatewayName(gwP95Entity))}</b></div>
        <div class="capacity-metric"><span>Carga agregada da infraestrutura</span><b>${pct(op.portfolio_current_occupancy_nominal)} <small>contexto, não folga da fila</small></b></div>
      </div>
    </div>
    ${opctx.afterHours?`<div class="capacity-context-note">Fora da janela: o estado atual não é usado isoladamente para recomendar expansão; histórico e resiliência continuam analisáveis.</div>`:""}`;
}

function esc(v){
  return String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}
function toDatetimeLocalValue(value){
  const d=value?new Date(value):new Date();
  if(Number.isNaN(d.getTime()))return "";
  const pad=n=>String(n).padStart(2,"0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function openCustomPeriodModal(){
  const end=customRange?.end?new Date(customRange.end):new Date(lastAnalytics?.bounds?.end||Date.now());
  const defaultMinutes=currentPeriod==="custom"?periodMinutes():Math.min(periodMinutes(),24*60);
  const start=customRange?.start?new Date(customRange.start):new Date(end.getTime()-defaultMinutes*60000);
  $("customStart").value=toDatetimeLocalValue(start);
  $("customEnd").value=toDatetimeLocalValue(end);
  $("customPeriodError").hidden=true;
  const m=$("customPeriodModal");m.classList.add("open");m.setAttribute("aria-hidden","false");
  document.body.classList.add("analytics-modal-open");
  setTimeout(()=>$("customStart")?.focus(),0);
}
function closeCustomPeriodModal(){
  const m=$("customPeriodModal");if(!m)return;
  m.classList.remove("open");m.setAttribute("aria-hidden","true");
  if(!$("managerialAnalyticsModal")?.classList.contains("open"))document.body.classList.remove("analytics-modal-open");
}
function applyCustomPeriod(){
  const start=$("customStart").value,end=$("customEnd").value,err=$("customPeriodError");
  const a=new Date(start),b=new Date(end);
  if(!start||!end||Number.isNaN(a.getTime())||Number.isNaN(b.getTime())||b<=a){
    err.textContent="Informe início e fim válidos; o fim deve ser posterior ao início.";err.hidden=false;return;
  }
  const min=(b-a)/60000;
  if(min>31*24*60){
    err.textContent="O intervalo personalizado está limitado a 31 dias nesta versão.";err.hidden=false;return;
  }
  customRange={start,end};currentPeriod="custom";
  document.querySelectorAll(".periods button").forEach(x=>x.classList.remove("active"));
  $("customPeriodBtn").classList.add("active");
  closeCustomPeriodModal();loadNow();
}
function closeManagerialAnalytics(){
  stopGatewayCurrentRefresh();
  const m=$("managerialAnalyticsModal");if(!m)return;
  m.classList.remove("open");m.setAttribute("aria-hidden","true");
  document.body.classList.remove("analytics-modal-open");
  document.querySelectorAll(".nav.analytics-selected").forEach(x=>x.classList.remove("analytics-selected"));
}
function analyticsCoverageChip(cov,label){
  const pctv=finiteNumber(cov?.data_coverage_pct??cov?.coverage_pct)??0;
  const cls=pctv>=95?"ok":pctv>=70?"warn":"bad";
  return `<span class="analytics-chip ${cls}">${esc(label)} ${fmt(pctv,2)}%</span>`;
}
function analyticsStatCard(label,value,sub=""){
  return `<div class="analytics-stat"><span>${esc(label)}</span><b>${value}</b>${sub?`<small>${sub}</small>`:""}</div>`;
}
function episodeTable(rows,kind){
  if(!rows?.length)return '<div class="analytics-empty">Nenhum episódio identificado no intervalo selecionado.</div>';
  const head=kind==="capacity"
    ?'<div class="analytics-table-head"><span>Início</span><span>Fim</span><span>Duração</span><span>Pico ocupação</span></div>'
    :'<div class="analytics-table-head"><span>Início</span><span>Fim</span><span>Duração</span><span>Máx simultâneo</span></div>';
  const body=rows.slice().reverse().slice(0,30).map(x=>`<div class="analytics-table-row">
    <span>${esc(String(x.start||"").slice(5,16).replace("T"," "))}</span>
    <span>${esc(String(x.end||"").slice(5,16).replace("T"," "))}</span>
    <span>${humanMinutes(x.duration_min)}</span>
    <span title="${x.max_ts?`Pico em ${eventStamp(x.max_ts)}`:''}">${kind==="capacity"?pct(x.max):n0(x.max)}</span>
  </div>`).join("");
  return head+body;
}
function contextPhrase(ctx){
  if(!ctx||!ctx.samples)return 'sem amostras suficientes';
  const d=finiteNumber(ctx.calls_dialing?.mean),h=finiteNumber(ctx.hit_rate15?.mean),b=finiteNumber(ctx.buffer?.mean),a=finiteNumber(ctx.agents_available?.mean),p=finiteNumber(ctx.p135?.max);
  return `discando méd. ${d===null?'N/C':n0(d)} · Hit Rate 15' ${h===null?'N/C':pct(h)} · buffer ${b===null?'N/C':n0(b)} · agentes disponíveis ${a===null?'N/C':fmt(a,1)} · P135 pico ${p===null?'N/C':pct(p)}`;
}
function genericFeedbackForm(entity,placeholder){
  return `<div class="generic-feedback-wrap"><button class="generic-feedback-open" type="button">+ Adicionar informação operacional</button><form class="gateway-feedback-form generic-feedback-form" data-entity="${esc(entity)}" hidden>
    <div class="gateway-feedback-form-head"><b>Registrar conhecimento operacional</b><span>Registro append-only; não altera telemetria, diagnóstico ou ML.</span></div>
    <div class="gateway-feedback-grid">
      <label>Tipo<select name="event_type"><option value="OBSERVATION">Observação</option><option value="CAUSE">Causa</option><option value="INTERVENTION">Intervenção</option><option value="RECOVERY">Recuperação</option><option value="FALSE_POSITIVE">Falso positivo</option></select></label>
      <label>Data/hora do evento<input name="event_time" type="datetime-local"></label>
      <label>Precisão<select name="time_precision"><option value="EXACT">Exata</option><option value="APPROXIMATE">Aproximada</option><option value="UNKNOWN">Não sei</option></select></label>
      <label>Causalidade<select name="causality"><option value="UNKNOWN">Não definida</option><option value="CONFIRMED">Confirmada</option><option value="PROBABLE">Provável</option><option value="HYPOTHESIS">Hipótese</option><option value="NOT_APPLICABLE">Não se aplica</option></select></label>
      <label>Operador<input name="operator" value="TOI Admin" maxlength="200"></label><label>Fonte<input name="source" placeholder="Operação / fornecedor / gestor" maxlength="300"></label>
    </div>
    <label class="gateway-feedback-wide">O que aconteceu / causa / ação<textarea name="description" required maxlength="4000" placeholder="${esc(placeholder)}"></textarea></label>
    <label class="gateway-feedback-wide">Resultado<input name="result" maxlength="1000" placeholder="Ex.: capacidade normalizou e permaneceu estável"></label>
    <label class="gateway-feedback-wide">Observação adicional<textarea name="notes" maxlength="2000"></textarea></label>
    <div class="gateway-feedback-actions"><button type="submit">Salvar registro</button><button type="button" class="generic-feedback-cancel">Cancelar</button><span class="gateway-feedback-status"></span></div>
  </form></div>`;
}
function bindGenericFeedbackActions(rerender){
  document.querySelectorAll('.generic-feedback-open').forEach(btn=>btn.addEventListener('click',()=>{const f=btn.parentElement?.querySelector('.generic-feedback-form');if(f)f.hidden=false}));
  document.querySelectorAll('.generic-feedback-cancel').forEach(btn=>btn.addEventListener('click',()=>{const f=btn.closest('form');if(f)f.hidden=true}));
  document.querySelectorAll('.generic-feedback-form').forEach(form=>form.addEventListener('submit',async ev=>{
    ev.preventDefault();const fd=new FormData(form),status=form.querySelector('.gateway-feedback-status'),submit=form.querySelector('button[type="submit"]');
    const payload={instancia:$('instanceSelect').value,entity:form.dataset.entity,event_type:fd.get('event_type'),event_time:fd.get('event_time')||null,time_precision:fd.get('time_precision'),causality:fd.get('causality'),operator:fd.get('operator'),source:fd.get('source'),description:fd.get('description'),result:fd.get('result'),notes:fd.get('notes')};
    try{submit.disabled=true;status.textContent='Salvando…';await api('/api/operational/feedback',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});status.textContent='Registro salvo.';if(rerender)await rerender();}
    catch(e){status.textContent=`Falha: ${e.message||e}`;submit.disabled=false;}
  }));
}
function renderStuckManagerial(a){
  const s=a?.stuck_analytics||{},cov=s.coverage||a?.coverage?.stuck||{},story=a?.event_intelligence?.stuck_episode_story||[];
  const peak=a?.event_intelligence?.peaks?.stuck||{},worst=story[0]||null,entity=`STUCK_CHANNELS:${a?.instancia||$('instanceSelect').value}`;
  const feedback=a?.operational_feedback?.stuck_channels?.records||[];
  $("analyticsModalEyebrow").textContent="INTELIGÊNCIA · CANAIS TRAVADOS · EVENTOS";
  $("analyticsModalTitle").textContent="Canais Travados · o que aconteceu no período";
  $("analyticsModalSubtitle").textContent=`Instância ${esc(a?.instancia||'—')} · ${esc(a?.period_label||periodLabel())} · fonte informa quantidade por Instância; não identifica canais individualmente.`;
  $("analyticsModalToolbar").innerHTML=analyticsCoverageChip(cov,"IPBX")+`<span class="analytics-chip neutral">${n0(cov.samples)} amostras</span><span class="analytics-chip neutral">${n0(cov.gap_count)} gaps</span>`;
  const feedbackHtml=feedback.length?`<section class="analytics-section"><h4>Conhecimento informado pela operação</h4>${feedback.map(r=>`<div class="operational-feedback-row"><b>${esc(feedbackLabel(r.event_type))}</b> · ${esc(r.event_time?operationalDateTime(r.event_time,r.time_precision):operationalDateTime(r.recorded_at))} · ${esc(r.description||'')}${r.result?` · Resultado: ${esc(r.result)}`:''}</div>`).join('')}</section>`:'';
  const worstCtx=worst?.queue_context||{};
  $("analyticsModalBody").innerHTML=`
    <section class="operational-story-banner"><b>Leitura operacional:</b> Foram observados ${n0(s.episodes_count)} episódios. No pior instante, ${n0(s.max)} canais ficaram travados simultaneamente${peak.ts?` em ${eventStamp(peak.ts)}`:''}. O maior episódio durou ${humanMinutes(s.max_episode_minutes)} e a exposição acumulada foi ${fmt(s.channel_minutes,0)} canal-min.</section>
    <section class="analytics-stat-grid">
      ${analyticsStatCard('Máx. simultâneo',n0(s.max),peak.ts?`pico em ${eventStamp(peak.ts)}`:periodLabel())}
      ${analyticsStatCard('Média simultânea',fmt(s.mean,1),'canais simultâneos nas amostras observadas')}
      ${analyticsStatCard('Episódios',n0(s.episodes_count),'períodos contínuos com quantidade > 0')}
      ${analyticsStatCard('Tempo com ocorrência',humanMinutes(s.affected_minutes),'pelo menos um canal travado')}
      ${analyticsStatCard('Exposição',`${fmt(s.channel_minutes,0)} canal-min`,'impacto acumulado de quantidade × tempo')}
      ${analyticsStatCard('Maior episódio',humanMinutes(s.max_episode_minutes),'duração observada/estimada pela cadência')}
    </section>
    <section class="parking-analogy"><h4>Entenda como um estacionamento</h4><p>Pense em cada canal como uma <b>vaga de estacionamento</b>. Quando um canal trava, uma vaga fica interditada. Cada vaga interditada durante 1 minuto soma <b>1 canal-min</b>. Assim, 10 canais travados durante 5 minutos equivalem a 50 canal-min. O canal-min mede a <b>exposição acumulada da capacidade</b>; não é quantidade de canais únicos.</p><p>Por isso o TOI sempre apresenta junto o <b>pico simultâneo</b>, a <b>duração</b> e o número de <b>episódios</b>: 50 canal-min podem ser 1 canal por 50 min ou 10 canais por 5 min — impactos com formatos diferentes.</p></section>
    ${worst?`<section class="event-story"><h4>Maior impacto acumulado identificado</h4><div class="event-story-grid"><div><b>Antes</b><p>${esc(contextPhrase(worstCtx.before))}</p></div><div><b>Durante</b><p>${n0(worst.max)} canais no pico · ${humanMinutes(worst.duration_min)} · ${fmt(worst.channel_minutes,0)} canal-min. Contexto: ${esc(contextPhrase(worstCtx.during))}</p></div><div><b>Depois</b><p>${esc(contextPhrase(worstCtx.after))}</p></div></div><small>Coincidência temporal entre sinais é contexto; não comprova causalidade.</small></section>`:''}
    <section class="analytics-section"><h4>Episódios do período</h4><div class="analytics-table">${episodeTable(s.episodes||[],'stuck')}</div></section>
    <section class="analytics-section"><h4>Qualidade temporal</h4><div class="analytics-facts"><span>Cadência mediana <b>${fmt(cov.median_cadence_s,1)}s</b></span><span>Maior gap <b>${fmt(cov.largest_gap_min,1)} min</b></span><span>Dias observados <b>${n0(cov.days_observed)}</b></span><span>Cobertura útil <b>${fmt(cov.data_coverage_pct??cov.coverage_pct,2)}%</b></span></div><p class="semantic-note">Intervalos sem dados não são convertidos em zero. A fonte atual não permite contar canais físicos únicos.</p></section>
    ${feedbackHtml}${genericFeedbackForm(entity,'Ex.: fornecedor informou causa do travamento; ação aplicada e horário aproximado.')}`;
  bindGenericFeedbackActions(async()=>{lastAnalytics=await api(analyticsUrl());renderStuckManagerial(lastAnalytics)});
}

function capacityStateTone(v){
  const s=String(v||"N/C").toUpperCase();
  if(["ALTO","REDUZIDA","SEM_FOLGA_IMEDIATA","SEM FOLGA IMEDIATA","AGRAVANTE","P34 DOMINANTE","P135 DOMINANTE","EPISÓDIO CRÍTICO","PRESSÃO ALTO"].includes(s))return "bad";
  if(["MÉDIO","ATENÇÃO","PLANEJADA","STANDBY MAPEADO","PARCIAL","EM FORMAÇÃO","ATIVA","PRESSÃO OBSERVADA","PRESENTE","PRESSÃO MÉDIO"].includes(s))return "warn";
  if(["BAIXO","ADEQUADA","COMPLETA","SEM RESTRIÇÃO CONHECIDA","AUSENTE","SEM_EVIDÊNCIA","SEM EVIDÊNCIA","BAIXA RELEVÂNCIA","FOLGA PRESENTE","SEM ATIVIDADE","PRESSÃO BAIXO"].includes(s))return "ok";
  return "neutral";
}
function capacityDimensionDetail(key,d){
  if(!d)return "Sem evidência";
  if(key==="demand")return `Período: discando méd. ${n0(d.period_calls_dialing_mean)} · pico ${n0(d.period_calls_dialing_peak)} · Hit 15' méd. ${pct(d.period_hit_rate15_mean)} · Agora: ${n0(d.calls_dialing)} discando`;
  if(key==="attendance")return `Agora: disponíveis ${n0(d.agents_available)} · falando ${n0(d.agents_talking)} · em atendimento ${n0(d.calls_in_attendance)}`;
  if(key==="trunks")return `Período: 95% das leituras de ${esc(d.historical_pressure_group||"—")} ficaram até ${pct(d.critical_group_p95_pct)} · folga nesse nível ${pct(d.critical_group_headroom_p95_pct)} · grupo com menor valor: ${esc(d.lowest_p95_group||"—")} ${pct(d.lowest_p95_pct)} · Agora: ${esc(d.critical_group||"—")} ${pct(d.critical_group_current_pct)}`;
  if(key==="gateways")return `Agora: ${esc(displayGatewayName(d.current_max_entity||"—"))} ${pct(d.current_max_pct)} · Período: 95% das leituras de ${esc(displayGatewayName(d.p95_entity||"—"))} ficaram até ${pct(d.p95_pct)} · Gateway com menor valor: ${esc(displayGatewayName(d.lowest_p95_entity||"—"))} ${pct(d.lowest_p95_pct)} · falha confirmada ${n0(d.fault_confirmed)}`;
  if(key==="stuck_channels")return `Agora ${n0(d.current)} · pico ${n0(d.max)} · materialidade ${d.materiality_pct===null||d.materiality_pct===undefined?'N/C':pct(d.materiality_pct)}`;
  if(key==="congestion")return `Período: P34 95% das leituras ficaram até ${pct(d.p34_period_p95)} · pico ${pct(d.p34_period_peak)} · ≥20% ${humanMinutes(d.p34_minutes_over_20)} | P135 95% das leituras ficaram até ${pct(d.p135_period_p95)} · pico ${pct(d.p135_period_peak)} · ≥20% ${humanMinutes(d.p135_minutes_over_20)} · Agora P34 ${pct(d.p34_pct)} / P135 ${pct(d.p135_pct)}`;
  if(key==="resilience"){
    const why=Array.isArray(d.confidence_reasons)&&d.confidence_reasons.length?` · ${d.confidence_reasons.join(" · ")}`:"";
    return `Confiança ${esc(d.confidence||"N/C")} · ${esc(d.contingency_headroom_status||"contingência N/C")}${esc(why)}`;
  }
  return esc(d.note||"");
}
function renderCapacityManagerial(a){
  const c=a?.capacity_analytics||{},op=c.operational||{},cov=c.coverage||a?.coverage?.trunk||{};
  const trunks=a?.infra_context?.relevant_trunks||[];
  const gateways=a?.infra_context?.relevant_gateways||[];
  const dims=op.dimensions||c.dimensions||{};
  const contexts=op.gateway_contexts||c.gateway_contexts||a?.gateway_semantic_context?.gateways||[];
  const groupProfiles=op.trunk_group_profiles||c.trunk_group_profiles||[];
  const gatewayProfiles=op.gateway_profiles||c.gateway_profiles||[];
  const critical=op.critical_group||c.critical_group||{};
  const hist=op.historical_pressure_group||c.historical_pressure_group||{};
  const lowGroup=op.lowest_pressure_group||{};
  const lowGwEntity=op.gateway_lowest_p95_entity||"";
  const lowGwP95=finiteNumber(op.gateway_lowest_p95);
  const status=op.overall_status||"N/C";
  const resilience=op.resilience_status||c.resilience_status||"N/C";
  const confidence=op.evidence_confidence||c.evidence_confidence||"N/C";
  const evidenceReasons=op.evidence_reasons||[];
  const evidenceCoverage=finiteNumber(op.evidence_coverage_pct??cov.data_coverage_pct??cov.coverage_pct);

  $("analyticsModalEyebrow").textContent="INTELIGÊNCIA · PRESSÃO DE CAPACIDADE";
  $("analyticsModalTitle").textContent="Pressão de capacidade, headroom e gargalos";
  const scopeMode=c.scope_mode||"INSTANCE_FALLBACK";
  $("analyticsModalSubtitle").textContent=scopeMode==="QUEUE_MAPPED_BY_LASTCALLS"
    ?`Instância ${a?.instancia||"—"} · ${a?.period_label||periodLabel()} · infraestrutura relacionada à fila por evidência de Last Calls`
    :`Instância ${a?.instancia||"—"} · ${a?.period_label||periodLabel()} · infraestrutura da Instância usada como contexto observacional`;

  $("analyticsModalToolbar").innerHTML=
    analyticsCoverageChip(cov,"COBERTURA TRONCOS")+
    `<span class="analytics-chip ${capacityStateTone(status)}">PRESSÃO ${esc(status)}</span>`+
    `<span class="analytics-chip ${capacityStateTone(resilience)}">RESILIÊNCIA ${esc(resilience)}</span>`+
    `<span class="analytics-chip ${capacityStateTone(confidence)}">EVIDÊNCIA ${esc(confidence)}</span>`+
    `<span class="analytics-chip neutral">${n0(c.relevant_trunk_groups)} grupos</span>`+
    `<span class="analytics-chip neutral">${n0(gateways.length)} gateways relacionados</span>`;

  const trunkRows=(groupProfiles.length?groupProfiles:trunks.map(x=>({
      entity:x.entity,current_pct:x.occupancy,p95_pct:null,headroom_p95_pct:null,busy:x.busy,total:x.total,minutes_over_90:null
    }))).length
    ?`<div class="capacity-resource-head trunk"><span>Grupo</span><span>Agora</span><span>95% das leituras ficaram até</span><span>Folga no nível que 95% das leituras não ultrapassaram</span><span>Busy / Total</span><span>Tempo ≥90%</span></div>`+
      (groupProfiles.length?groupProfiles:trunks.map(x=>({
        entity:x.entity,current_pct:x.occupancy,p95_pct:null,headroom_p95_pct:null,busy:x.busy,total:x.total,minutes_over_90:null
      }))).map(x=>`<div class="capacity-resource-row trunk">
        <span>${esc(x.entity)}</span><span>${pct(x.current_pct)}</span><span>${pct(x.p95_pct)}</span>
        <span>${pct(x.headroom_p95_pct)}</span><span>${n0(x.busy)} / ${n0(x.total)}</span><span>${humanMinutes(x.minutes_over_90)}</span>
      </div>`).join("")
    :'<div class="analytics-empty">Sem grupos de troncos relacionados no contexto atual.</div>';

  const gatewayByEntity=new Map((gatewayProfiles||[]).map(x=>[normEntityName(x.entity),x]));
  const contextByEntity=new Map((contexts||[]).map(x=>[normEntityName(x.entity),x]));
  const gwEntities=[...new Set([...(gateways||[]).map(x=>x.entity),...(contexts||[]).map(x=>x.entity)])];
  const gatewayRows=gwEntities.length
    ?`<div class="capacity-resource-head gateway"><span>Gateway</span><span>Agora</span><span>95% das leituras ficaram até</span><span>Técnico / ICMP agora</span><span>Contexto operacional</span><span>Tratamento na capacidade</span></div>`+
      gwEntities.map(entity=>{
        const p=gatewayByEntity.get(normEntityName(entity))||{};
        const h=healthForGateway(entity)||contextByEntity.get(normEntityName(entity))||{};
        const tech=gatewayTechnicalStatusLabel(h),techTone=gatewayTechnicalTone(h);
        const ctx=gatewayContextLabel(h),ctxTone=gatewayContextTone(h);
        const reach=h?.live_available===false?"Live N/C":gatewayReachabilityLabel(h).split(" · ")[0];
        const tip=esc(gatewayTooltip(h,tech));
        return `<div class="capacity-resource-row gateway">
          <span>${esc(displayGatewayName(entity))}</span><span>${pct(p.current_pct)}</span><span>${pct(p.p95_pct)}</span>
          <span><i class="gateway-health-state ${techTone}" title="${tip}">${esc(tech==="N/C"?"N/C":`${tech} · ${reach}`)}</i></span>
          <span title="${tip}"><i class="gateway-health-state ${ctxTone}">${esc(ctx)}</i> · ${esc(gatewayTrafficLabel(h))}</span>
          <span class="gateway-capacity-treatment ${ctxTone}" title="${tip}">${esc(gatewayCapacityLabel(h))}</span>
        </div>`;
      }).join("")
    :'<div class="analytics-empty">Sem Gateways relacionados no contexto atual.</div>';

  const dimOrder=["demand","attendance","trunks","gateways","stuck_channels","congestion","resilience"];
  const dimRows=dimOrder.filter(k=>dims[k]).map(k=>{
    const d=dims[k],tone=capacityStateTone(d.state);
    return `<div class="capacity-dimension-row">
      <span class="capacity-dimension-name">${esc(d.label||k)}</span>
      <span><i class="capacity-state ${tone}">${esc(d.state||"N/C")}</i></span>
      <span>${capacityDimensionDetail(k,d)}</span>
      <span title="${esc(d.note||"")}">${esc(d.note||"")}</span>
    </div>`;
  }).join("")||'<div class="analytics-empty">Sem dimensões calculadas para a janela.</div>';

  const bottleneckCatalog=(op.bottleneck_catalog||[]).map(x=>`<div class="operational-feedback-row"><b>${esc(x.label)}</b> · ${esc(x.meaning||'')}</div>`).join('');

  const faultCtx=(contexts||[]).filter(x=>String(x.canonical_status||"").toUpperCase()==="FAULT_CONFIRMED_CONTEXT");
  const plannedCtx=(contexts||[]).filter(x=>String(x.canonical_status||"").toUpperCase()==="PLANNED_INACTIVE");
  const standbyCtx=(contexts||[]).filter(x=>String(x.canonical_status||"").toUpperCase()==="STANDBY_EXPECTED");
  const partialCtx=(contexts||[]).filter(x=>String(x.canonical_status||"").toUpperCase()==="OPERATIONAL_PARTIAL_VISIBILITY");

  $("analyticsModalBody").innerHTML=`
    <section class="analytics-stat-grid capacity-stat-grid">
      ${analyticsStatCard("Pressão de capacidade",`<em class="analytics-risk ${capacityStateTone(status)}">${esc(status)}</em>`,op.bottleneck_label||"")}
      ${analyticsStatCard("Ocupação usada na classificação Telecom",pct(op.telecom_risk_basis?.occupancy_pct),`${esc(op.telecom_risk_basis?.source==='GATEWAY'?'Gateway':op.telecom_risk_basis?.source==='TRUNK'?'Grupo de troncos':'N/C')} · ${esc(displayGatewayName(op.telecom_risk_basis?.entity||'N/C'))}`)}
      ${analyticsStatCard("No período · grupo com maior valor não ultrapassado em 95% das leituras",pct(op.p95_occupancy),esc(hist.entity||critical.entity||"—"))}
      ${analyticsStatCard("Folga no grupo crítico quando 95% das leituras ficaram até esse uso",pct(op.headroom),esc(hist.entity||critical.entity||"—"))}
      ${analyticsStatCard("Agora · grupo mais ocupado",pct(critical.occupancy),esc(critical.entity||"—"))}
      ${analyticsStatCard("Agora · Gateway mais ocupado",pct(op.gateway_current_max),esc(displayGatewayName(op.gateway_current_max_entity||"—")))}
      ${analyticsStatCard("No período · Gateway com maior valor não ultrapassado em 95% das leituras",pct(op.gateway_p95),esc(displayGatewayName(op.gateway_p95_entity||"—")))}
      ${analyticsStatCard("No período · grupo mais ocioso (menor valor que 95% das leituras não ultrapassaram)",pct(lowGroup.p95_pct),`${esc(lowGroup.entity||"—")} · comparativo, não folga comprovada`)}
      ${analyticsStatCard("No período · Gateway mais ocioso (menor valor que 95% das leituras não ultrapassaram)",lowGwP95===null?'—':pct(lowGwP95),`${esc(displayGatewayName(lowGwEntity||"—"))} · exclui parada planejada; não prova rota alternativa`)}
      ${analyticsStatCard("Carga agregada da infraestrutura",pct(op.portfolio_current_occupancy_nominal),`${n0(op.portfolio_nominal_busy)} / ${n0(op.portfolio_nominal_total)} canais · contexto, não folga da fila`)}
      ${analyticsStatCard("Confiança da evidência",`<em class="analytics-risk ${capacityStateTone(confidence)}">${esc(confidence)}</em>`,`${evidenceCoverage===null?"cobertura N/C":fmt(evidenceCoverage,2)+"% cobertura"}${evidenceReasons.length?" · "+esc(evidenceReasons.join(" · ")):""}`)}
    </section>

    <section class="analytics-explain capacity-executive-read">
      <h4>Leitura operacional do período</h4>
      <p><b>O que a pressão de capacidade significa:</b> quanto a infraestrutura observada se aproximou do limite no período selecionado. Um grupo crítico pode estar saturado mesmo quando a carga agregada da Instância parece baixa.</p>
      <p><b>Como classificamos a pressão Telecom:</b> usamos a maior ocupação sustentada válida entre os grupos de troncos e Gateways do escopo. Gateways exigem ao menos 60% de cobertura e cinco leituras válidas; as duas fontes não são somadas.</p>
      <p><b>Gargalo dominante observado:</b> ${esc(op.bottleneck_label||"Nenhum gargalo dominante comprovado")}. ${esc(op.bottleneck_reason||"")}</p>
      <p><b>No período:</b> 95% das leituras de ${esc(hist.entity||critical.entity||"grupo N/C")} ficaram em até ${pct(op.p95_occupancy)}, com folga de ${pct(op.headroom)} nesse nível. <b>Agora:</b> ${esc(critical.entity||"grupo N/C")} está em ${pct(critical.occupancy)}.</p>
      <p><b>Comparativo de grupos:</b> em ${esc(lowGroup.entity||"N/C")}, 95% das leituras ficaram em até ${pct(lowGroup.p95_pct)}, o menor valor entre os grupos. Menor pressão observada não significa capacidade intercambiável ou rota alternativa para esta fila.</p>
      <p><b>Gateways:</b> agora ${esc(displayGatewayName(op.gateway_current_max_entity||"N/C"))} em ${pct(op.gateway_current_max)}; no período, 95% das leituras de ${esc(displayGatewayName(op.gateway_p95_entity||"N/C"))} ficaram em até ${pct(op.gateway_p95)}, o maior valor entre os Gateways. O menor foi ${esc(displayGatewayName(lowGwEntity||"N/C"))}: ${lowGwP95===null?'N/C':pct(lowGwP95)}.</p>
      <p>${esc(c.scope_note||"")}</p>
      <p><b>Próxima ação:</b> ${esc(op.recommendation||"Manter observação multidimensional.")}</p>
    </section>

    <section class="analytics-section">
      <h4>Quais gargalos o motor consegue classificar?</h4>
      <p class="analytics-section-note">A classificação é observacional e ranqueia a principal pressão disponível; não transforma coincidência temporal em causalidade.</p>
      ${bottleneckCatalog||'<div class="analytics-empty">Catálogo de gargalos indisponível nesta versão.</div>'}
    </section>

    <section class="analytics-section">
      <h4>Leitura por dimensão</h4>
      <div class="capacity-dimensions">
        <div class="capacity-dimension-head"><span>Dimensão</span><span>Leitura</span><span>O que foi observado</span><span>O que significa operacionalmente</span></div>
        ${dimRows}
      </div>
    </section>

    <section class="analytics-section">
      <h4>Grupos de troncos · pressão localizada versus ocupação nominal</h4>
      <p class="analytics-section-note">O maior percentual representa um grupo específico. O valor abaixo do qual ficaram 95% das leituras ajuda a comparar o uso, mas não prova que o grupo esteja disponível para esta fila. A ocupação nominal mapeada usa soma busy / soma total e não prova que a capacidade entre rotas seja intercambiável.</p>
      <div class="capacity-resources">${trunkRows}</div>
    </section>

    <section class="analytics-section">
      <h4>Gateways · estado operacional e efeito na capacidade</h4>
      <p class="analytics-section-note">Ground Truth operacional permanece visível mesmo com telemetria Shadow temporariamente indisponível. O menor valor abaixo do qual ficaram 95% das leituras serve apenas para comparar utilização; não prova capacidade alternativa. Falha confirmada reduz resiliência; standby não é folga ativa; parada planejada não conta como capacidade imediata; visibilidade parcial reduz confiança.</p>
      <div class="capacity-resources">${gatewayRows}</div>
    </section>

    <section class="analytics-section">
      <h4>Qualidade temporal e governança</h4>
      <div class="analytics-facts">
        <span>Cobertura útil <b>${fmt(cov.data_coverage_pct??cov.coverage_pct,2)}%</b></span>
        <span>Maior gap <b>${fmt(cov.largest_gap_min,1)} min</b></span>
        <span>Dias observados <b>${n0(cov.days_observed)}</b></span>
        <span>Perfil <b>${a?.mode==="PROFILE_24H"?"00:00–23:59":"cronológico"}</b></span>
      </div>
      <p class="analytics-governance-note">Somente leitura. A análise não altera aquisição, diagnóstico oficial, alerta, chamado ou ação. Ausência de vínculo direto fila→rota/gateway/canal é preservada como limitação, sem causalidade inventada.</p>
    </section>`;
}


function gatewayAnalyticsUrl(detail='summary',entity=null){
  const inst=$('instanceSelect').value,code=$('queueSelect').value;
  let base=`/api/gateway/analytics?instancia=${encodeURIComponent(inst)}&codigo=${encodeURIComponent(code||'')}&period=${encodeURIComponent(currentPeriod)}&detail=${encodeURIComponent(detail)}`;
  if(entity)base+=`&entity=${encodeURIComponent(entity)}`;
  if(currentPeriod!=="custom")return base;
  return `${base}&start=${encodeURIComponent(customRange?.start||"")}&end=${encodeURIComponent(customRange?.end||"")}`;
}
function entityMap(rows){return new Map((rows||[]).map(x=>[normEntityName(x.entity),x]))}
function gatewayHelp(label,text){return `${esc(label)} <span class="term-help" title="${esc(text)}" aria-label="${esc(text)}">?</span>`}
function operationalDateTime(v,precision='EXACT'){
  if(!v)return 'horário não registrado';
  const raw=String(v),d=new Date(raw);
  if(!Number.isNaN(d.getTime())&&raw.includes('T'))return d.toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
  if(/^\d{4}-\d{2}-\d{2}$/.test(raw)){const [y,m,dd]=raw.split('-');return `${dd}/${m}/${y}${String(precision).toUpperCase()==='DAY'?' (horário não registrado)':''}`}
  return raw;
}
function operationalDateTimeExact(v){
  if(!v)return 'horário não registrado';
  const raw=String(v),d=new Date(raw);
  if(!Number.isNaN(d.getTime())&&raw.includes('T'))return d.toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'});
  return operationalDateTime(v);
}
function gatewayPhaseText(st){
  if(!st||!st.samples)return 'Não há amostras suficientes neste trecho para concluir com segurança.';
  const q=st.quality||{},parts=[
    `ocupação média ${pct(st.occupancy_mean_pct)}`,
    `95% das leituras ficaram até ${pct(st.occupancy_p95_pct)}`,
    `pico ${pct(st.occupancy_peak_pct)}`
  ];
  if(finiteNumber(st.service_active_pct)!==null)parts.push(`com atividade ${pct(st.service_active_pct)}`);
  if(finiteNumber(st.zero_occupancy_pct)!==null)parts.push(`ocupação 0% em ${pct(st.zero_occupancy_pct)}`);
  if(finiteNumber(st.peer_mean_pct)!==null)parts.push(`pares em média ${pct(st.peer_mean_pct)}`);
  if(finiteNumber(st.peer_gap_mean_pct_points)!==null)parts.push(`diferença média p/ pares ${fmt(st.peer_gap_mean_pct_points,1)} p.p.`);
  if(finiteNumber(st.ping_ok_pct)!==null)parts.push(`Ping OK ${pct(st.ping_ok_pct)}`);
  if(finiteNumber(st.path_ok_pct)!==null)parts.push(`caminho OK ${pct(st.path_ok_pct)}`);
  if(finiteNumber(q.nominal_completeness_pct)!==null)parts.push(`completude nominal ${pct(q.nominal_completeness_pct)}`);
  if(finiteNumber(q.largest_gap_s)>0)parts.push(`maior gap ${fmt(q.largest_gap_s,0)}s`);
  return `${n0(st.samples)} amostras: ${parts.join(' · ')}.`;
}
function gatewayWindowLine(x){
  if(!x)return '<span class="entity-window-na">sem amostras</span>';
  const svc=x.service_state_pct||{},reach=x.reachability_pct||{},q=x.quality||{};
  const completeness=finiteNumber(q.nominal_completeness_pct),continuity=finiteNumber(q.continuity_coverage_pct);
  return `<b>ocupação média ${pct(x.occupancy_mean_pct)}</b> · ${gatewayHelp('95% das leituras ficaram até', 'Valor abaixo do qual ficaram 95% das medições observadas; reduz o peso de picos isolados.')} ${pct(x.occupancy_p95_pct)} · pico ${pct(x.occupancy_peak_pct)}<br>`+
    `com atividade ${pct(svc.ACTIVE)} · sem ocupação crítica ${pct(svc.SILENT_CRITICAL)} · Ping OK ${pct(reach.OK)}<br>`+
    `${n0(x.transitions)} mudanças de estado · completude ${completeness===null?pct(q.coverage_pct):pct(completeness)}${continuity!==null?` · continuidade ${pct(continuity)}`:''}${finiteNumber(q.largest_gap_s)>0?` · maior intervalo sem dado ${fmt(q.largest_gap_s,0)}s`:''}`;
}
function gatewaySelectedInterpretation(x,h){
  const latest=x?.latest||{},svc=String(latest.service_state||'').toUpperCase(),reach=String(latest.reachability_state||'').toUpperCase();
  const noOcc=finiteNumber(x?.current_functional_silence_min),sem=gatewaySemanticCode(h);
  if(sem==='FAULT_CONFIRMED_CONTEXT')return 'Existe falha operacional confirmada. Mesmo com Ping respondendo, este Gateway não deve ser considerado saudável até revalidação operacional.';
  if(sem==='RECOVERED_CONFIRMED')return 'A operação confirmou recuperação. O histórico da falha permanece preservado e o comportamento posterior deve ser acompanhado.';
  if(sem==='STANDBY_EXPECTED')return 'Este Gateway é standby/SBC de última rota; baixa ocupação pode ser normal e não representa automaticamente perda de capacidade.';
  if(sem==='PLANNED_INACTIVE')return 'Parada planejada; o equipamento não deve ser tratado como capacidade imediatamente disponível.';
  if(sem==='OPERATIONAL_PARTIAL_VISIBILITY')return 'O TOI enxerga apenas parte do tráfego deste Gateway; ausência de ocupação reduz a confiança, mas não permite concluir ausência total de uso.';
  if(svc==='SILENT_CRITICAL'&&reach==='OK')return `O Gateway responde ao Ping, mas o TOI não observa ocupação${noOcc!==null?` há ${fmt(noOcc,1)} min`:''}. Compare com os demais Gateways e verifique serviço/rota antes de atribuir causa ao equipamento.`;
  if(svc==='SILENT_WARN'&&reach==='OK')return 'O Gateway responde ao Ping, porém está há mais tempo que o esperado sem ocupação. Acompanhar persistência e comparação com os demais Gateways.';
  if(svc==='ACTIVE'&&reach==='OK')return 'Há atividade e o Ping responde no fim da janela. Avalie recorrência, pressão e estabilidade ao longo do período.';
  if(reach==='PATH_UNCERTAIN')return 'O Ping individual está inconclusivo porque o caminho de monitoramento apresentou degradação; não atribuir falha ao Gateway sem outra evidência.';
  return 'Leitura operacional do período sem causa confirmada. Use a história abaixo e a cobertura dos dados para avaliar o equipamento.';
}
function gatewayReasonText(x){
  const r=String(x?.latest?.reason||'').trim();
  return r||'Sem razão técnica registrada para a última amostra da janela.';
}
function feedbackLabel(t){return ({OBSERVATION:'Observação',CAUSE:'Causa informada',INTERVENTION:'Intervenção',RECOVERY:'Recuperação',FALSE_POSITIVE:'Falso positivo'})[String(t||'').toUpperCase()]||String(t||'Registro')}
function causalityLabel(v){return ({CONFIRMED:'confirmada',PROBABLE:'provável',HYPOTHESIS:'hipótese',NOT_APPLICABLE:'não se aplica',UNKNOWN:'não definida'})[String(v||'UNKNOWN').toUpperCase()]||String(v||'')}
function gatewayLifecycleFaults(h,selectedStart,selectedEnd){
  const sem=h?.semantic_context||h||{},startDate=String(selectedStart||'').slice(0,10),endDate=String(selectedEnd||'').slice(0,10);
  return (sem.lifecycle_history||[]).filter(e=>{
    if(!`${e?.canonical_status||''} ${e?.lifecycle_state||''}`.toUpperCase().includes('FAULT'))return false;
    const a=String(e?.valid_from||'').slice(0,10),b=String(e?.valid_to||e?.valid_from||'').slice(0,10);
    if(!startDate||!endDate||!a)return true;
    return b>=startDate&&a<=endDate;
  });
}
function gatewayTechnicalFromAnalytics(x){
  const l=x?.latest||{},reach=String(l.reachability_state||'').toUpperCase(),path=String(l.path_state||'').toUpperCase();
  if(!l.ts)return {label:'N/C',tone:'neutral'};
  if(path==='DEGRADED'&&reach==='OK')return {label:'Ping OK · caminho degradado',tone:'warn'};
  if(path==='RECOVERY'&&reach==='OK')return {label:'Ping OK · caminho em recuperação',tone:'warn'};
  if(reach==='PATH_UNCERTAIN')return {label:'Ping inconclusivo · caminho degradado',tone:'warn'};
  if(reach==='INTERMITTENT')return {label:'Ping intermitente',tone:'warn'};
  if(reach==='DOWN')return {label:'Ping indisponível',tone:'bad'};
  if(reach==='OK')return {label:'Ping OK',tone:'ok'};
  return {label:reach||'N/C',tone:'neutral'};
}
function gatewayPeerSignal(x){
  const d=finiteNumber(x?.peer_divergence_peak_pct_points);
  if(d===null)return {text:'comparação com pares N/C',tone:'neutral'};
  if(d>=30)return {text:`divergência máx. ${fmt(d,1)} p.p.`,tone:'bad'};
  if(d>=15)return {text:`divergência máx. ${fmt(d,1)} p.p.`,tone:'warn'};
  return {text:`pares próximos · máx. ${fmt(d,1)} p.p.`,tone:'ok'};
}
function gatewaySummaryHeadline(x,h){
  const sem=gatewaySemanticCode(h),cov=finiteNumber(x?.quality?.coverage_pct),div=finiteNumber(x?.peer_divergence_peak_pct_points),svc=x?.service_state_pct||{},crit=finiteNumber(svc.SILENT_CRITICAL),warn=finiteNumber(svc.SILENT_WARN),peak=finiteNumber(x?.occupancy_peak_pct);
  if(sem==='FAULT_CONFIRMED_CONTEXT')return 'Falha operacional confirmada; equipamento não deve ser tratado como redundância saudável.';
  if(sem==='RECOVERED_CONFIRMED'&&div!==null&&div>=30)return `Recuperado confirmado; no período chegou a ficar ${fmt(div,1)} p.p. abaixo dos Gateways comparáveis.`;
  if(sem==='RECOVERED_CONFIRMED')return 'Recuperação confirmada; ocorrência histórica preservada para comparação.';
  if(sem==='STANDBY_EXPECTED')return 'Gateway de standby; baixa ocupação pode ser esperada e precisa ser lida nesse contexto.';
  if(sem==='PLANNED_INACTIVE')return 'Parada planejada; não considerar como capacidade operacional imediata.';
  if(!x?.samples)return 'Sem amostras de análise neste período. Consulte a ocupação LIVE ao lado.';
  if(cov!==null&&cov<20)return 'Pouca evidência na janela operacional; não concluir comportamento a partir deste período.';
  if(crit!==null&&crit>0)return 'Houve períodos de ausência de ocupação em nível crítico dentro da janela operacional.';
  if(warn!==null&&warn>0)return 'Houve períodos de atenção por ausência de ocupação dentro da janela operacional.';
  if(div!==null&&div>=30)return `Comportamento destoou dos pares em até ${fmt(div,1)} p.p. no período.`;
  if(peak!==null&&peak>0)return `Operou na janela selecionada; maior ocupação observada ${pct(peak)}.`;
  return 'Sem ocupação relevante observada na janela operacional; confirme cobertura e papel do equipamento.';
}
function gatewaySummaryPriority(x,h){
  const sem=gatewaySemanticCode(h),div=finiteNumber(x?.peer_divergence_peak_pct_points)||0,crit=finiteNumber(x?.service_state_pct?.SILENT_CRITICAL)||0,warn=finiteNumber(x?.service_state_pct?.SILENT_WARN)||0;
  if(sem==='FAULT_CONFIRMED_CONTEXT')return 1000;
  if(sem==='RECOVERED_CONFIRMED')return 800+div;
  if(crit>0)return 700+crit;
  if(div>=30)return 600+div;
  if(warn>0)return 500+warn;
  if(sem==='STANDBY_EXPECTED')return 100;
  return div;
}
function gatewayEventTimelineHtml(x,story,selectedStart,selectedEnd){
  const startMs=new Date(selectedStart||'').getTime(),endMs=new Date(selectedEnd||'').getTime();
  if(!Number.isFinite(startMs)||!Number.isFinite(endMs)||endMs<=startMs)return '';
  const items=[];
  const timelineEpisodes=Array.isArray(x?.zero_occupancy_episodes)?x.zero_occupancy_episodes:(Array.isArray(x?.silence_episodes)?x.silence_episodes:[]);
  for(const e of timelineEpisodes){
    const endTs=e.recovery_ts||e.first_positive_after_gap_ts||e.last_zero_ts||e.end;
    const a=new Date(e.start||'').getTime(),b=new Date(endTs||'').getTime();
    if(!Number.isFinite(a)||!Number.isFinite(b))continue;
    const mode=e.recovery_observed_after_gap?' · retorno após gap':'';
    items.push({a,b:Math.max(a,b),kind:'absence',label:`Ocupação 0% · ${humanMinutes(e.duration_min)}${mode}`});
  }
  if(story?.event_start&&String(story.fault_precision||'').toUpperCase()!=='DAY'){
    const a=new Date(story.event_start).getTime(),b=new Date(story.event_end||story.event_start).getTime();
    if(Number.isFinite(a))items.push({a,b:Number.isFinite(b)?Math.max(a,b):a,kind:'fault',label:'Falha operacional confirmada'});
  }
  if(!items.length)return `<div class="gateway-mini-timeline empty"><span>Nenhum evento operacional confirmado com horário preciso nesta janela.</span></div>`;
  const span=endMs-startMs;
  const marks=items.slice(-20).map(it=>{
    const left=Math.max(0,Math.min(100,(it.a-startMs)/span*100)),rawW=Math.max(0,(it.b-it.a)/span*100),width=Math.max(.6,Math.min(100-left,rawW||.6));
    return `<i class="${it.kind}" style="left:${left.toFixed(2)}%;width:${width.toFixed(2)}%" title="${esc(`${it.label} · ${operationalDateTime(new Date(it.a).toISOString())}`)}"></i>`;
  }).join('');
  return `<div class="gateway-mini-timeline"><div class="gateway-mini-track">${marks}</div><div class="gateway-mini-scale"><span>${esc(operationalDateTime(selectedStart))}</span><b>eventos observados</b><span>${esc(operationalDateTime(selectedEnd))}</span></div></div>`;
}
function gatewayHypothesesHtml(){
  return `<div class="gateway-hypotheses"><span>Próximas verificações</span><i>serviço/processo DS</i><i>SIP/rota upstream</i><i>licenciamento/canais</i><i>caminho de rede</i><i>Last Calls e Gateways pares</i></div>`;
}
function gatewayStoryHtml({name,x,h,cases,feedback,story,q2,current,periodPartial,periodCovPct,selectedLabel,selectedStart,selectedEnd,opWindow}){
  const faults=gatewayLifecycleFaults(h,selectedStart,selectedEnd),sem=gatewaySemanticCode(h),episodes=Array.isArray(x?.zero_occupancy_episodes)?x.zero_occupancy_episodes:(Array.isArray(x?.silence_episodes)?x.silence_episodes:[]);
  const clusters=Array.isArray(x?.zero_occupancy_incident_clusters)?x.zero_occupancy_incident_clusters:[];
  const primaryCluster=[...clusters].sort((a,b)=>(Number(b?.zero_duration_min)||0)-(Number(a?.zero_duration_min)||0))[0]||story?.zero_incident_cluster||null;
  const longest=[...episodes].sort((a,b)=>(Number(b?.duration_min)||0)-(Number(a?.duration_min)||0))[0]||null;
  const cause=[...feedback].reverse().find(r=>String(r?.event_type).toUpperCase()==='CAUSE');
  const intervention=[...feedback].reverse().find(r=>String(r?.event_type).toUpperCase()==='INTERVENTION');
  const recovery=[...feedback].reverse().find(r=>String(r?.event_type).toUpperCase()==='RECOVERY');
  const currentCtx=gatewayContextLabel(h),tech=gatewayTechnicalFromAnalytics(x),qual=x?.quality||{},techQual=x?.technical_quality||{};
  const semPayload=h?.semantic_context||h||{},recoveryDate=String(semPayload.effective_from||'').slice(0,10),selStartDate=String(selectedStart||'').slice(0,10),selEndDate=String(selectedEnd||'').slice(0,10),recoveryInWindow=!!(recoveryDate&&selStartDate&&selEndDate&&recoveryDate>=selStartDate&&recoveryDate<=selEndDate);
  const afterGap=finiteNumber(story?.after?.peer_gap_mean_pct_points);
  let headline='Nenhuma falha operacional confirmada no período observado.';
  if(faults.length&&sem==='RECOVERED_CONFIRMED'&&recoveryInWindow)headline=afterGap!==null&&afterGap>=20?'Recuperação confirmada, porém o comportamento após o retorno ainda ficou abaixo dos pares.':'Falha histórica seguida de recuperação confirmada dentro da janela.';
  else if(faults.length&&sem==='RECOVERED_CONFIRMED')headline='Falha confirmada dentro da janela; o estado atual está recuperado.';
  else if(!faults.length&&sem==='RECOVERED_CONFIRMED')headline='Sem falha confirmada nesta janela; estado atual recuperado de ocorrência anterior.';
  else if(sem==='FAULT_CONFIRMED_CONTEXT')headline='Falha operacional confirmada permanece registrada.';
  else if(primaryCluster&&Number(primaryCluster.zero_duration_min)>0)headline=`Foram observados ${n0(primaryCluster.episode_count)} episódios de ocupação 0%, somando ${humanMinutes(primaryCluster.zero_duration_min)} no principal evento da janela.`;
  else if(longest&&Number(longest.duration_min)>0)headline=`Foi observado período sem ocupação de até ${humanMinutes(longest.duration_min)} dentro da janela operacional.`;
  else if(sem==='STANDBY_EXPECTED')headline='Comportamento compatível com Gateway de standby, sujeito à validação da janela.';
  const fault=faults[faults.length-1]||null;
  let eventText='Nenhum evento de falha confirmado foi registrado nesta janela.';
  if(fault)eventText=`Falha operacional confirmada entre ${operationalDateTime(fault.valid_from,fault.precision)} e ${operationalDateTime(fault.valid_to,fault.precision)}.`;
  else if(primaryCluster){
    const inter=(primaryCluster.intermediate_positive_observations||[]).map(p=>`${operationalDateTime(p.ts)} (${pct(p.pct)})`).join(', ');
    const finalTs=primaryCluster.final_recovery_ts,finalPct=finiteNumber(primaryCluster.final_recovery_pct);
    const gapNote=primaryCluster.contains_gap?' Houve gaps/intervalos sem continuidade suficiente; o TOI não afirma ausência física contínua durante esses trechos.':'';
    const interNote=inter?` Ocupações positivas intermediárias observadas: ${inter}; elas não encerraram a recorrência porque novos episódios de 0% ocorreram em seguida.`:'';
    const finalNote=finalTs?` Após o último episódio, ocupação positiva foi observada em ${operationalDateTime(finalTs)}${finalPct!==null?` (${pct(finalPct)})`:''}.`:' O evento permaneceu sem retomada final observada no recorte.';
    eventText=`O principal evento começou com 0% observado em ${operationalDateTime(primaryCluster.start)}. Foram ${n0(primaryCluster.episode_count)} episódios raw de 0%, somando ${humanMinutes(primaryCluster.zero_duration_min)} ao longo de ${humanMinutes(primaryCluster.span_min)} entre o primeiro zero e o último zero (${operationalDateTime(primaryCluster.last_zero_ts)}).${interNote}${finalNote}${gapNote}`;
  }
  else if(longest){
    const firstPositive=longest.recovery_ts||longest.first_positive_after_gap_ts;
    const recoveryNote=longest.recovery_ts
      ?` Primeira ocupação positiva observada em ${operationalDateTime(longest.recovery_ts)} (${pct(longest.recovery_pct)}).`
      :(longest.first_positive_after_gap_ts
        ?` Primeira ocupação positiva observada em ${operationalDateTime(longest.first_positive_after_gap_ts)} (${pct(longest.first_positive_after_gap_pct)}), após gap de ${fmt(longest.recovery_gap_s,0)}s; o instante exato da transição não foi observado.`
        :' O episódio permaneceu aberto no fim do trecho observado.');
    eventText=`O maior episódio de ocupação 0% começou em ${operationalDateTime(longest.start)}; último zero observado em ${operationalDateTime(longest.last_zero_ts||longest.end)}; duração observada ${humanMinutes(longest.duration_min)}.${recoveryNote}`;
  }
  let beforeText=story?.before?`Nas 2H que antecederam o evento (${operationalDateTime(story.before.start)}–${operationalDateTime(story.before.end)}): ${gatewayPhaseText(story.before)}`:(fault||longest?'Não há recorte suficiente para caracterizar com segurança as 2H anteriores ao evento.':'Não houve um evento principal confirmado que exija comparação pré-evento.');
  if(story?.before_first_hour?.samples&&story?.before_last_hour?.samples){
    const a=finiteNumber(story.before_first_hour.occupancy_mean_pct),b=finiteNumber(story.before_last_hour.occupancy_mean_pct);
    if(a!==null&&b!==null)beforeText+=` Tendência pré-evento: média da última 1H ${pct(b)} versus ${pct(a)} na hora anterior (${b-a>=0?'+':''}${fmt(b-a,1)} p.p.).`;
  }
  const duringText=story?.during?gatewayPhaseText(story.during):(fault||longest?'O TOI preserva o evento, mas não possui amostras suficientes no trecho para quantificar seu comportamento sem inventar dados.':'Sem evento de falha confirmado.');
  const hasEvent=!!(fault||primaryCluster||longest||sem==='FAULT_CONFIRMED_CONTEXT');
  let causeText=hasEvent?'Causa ainda não identificada no TOI.':'Não há evento de falha confirmado nesta janela que exija atribuição de causa.';
  if(hasEvent&&cause)causeText=`${cause.description} (${causalityLabel(cause.causality)}; fonte: ${cause.source||'operador'}).`;
  else if(hasEvent&&cases.length){
    const c=cases[0],assoc=String(c.causality_status||'').includes('NOT_PROVEN');
    causeText=`Há precedente operacional relacionado: ${c.interpretation_template||c.operational_story_note||'caso registrado'}.${assoc?' A associação é temporal; a causa não foi comprovada.':''}`;
  }
  let solutionText=hasEvent?'Forma de resolução ainda não registrada.':'Não houve resolução a registrar para esta janela.';
  if(intervention)solutionText=`${intervention.description}${intervention.result?` Resultado informado: ${intervention.result}.`:'.'} ${intervention.event_time?`Intervenção registrada para ${operationalDateTime(intervention.event_time,intervention.time_precision)}.`:''}`;
  else if(recovery)solutionText=`Recuperação informada: ${recovery.description}${recovery.event_time?` em ${operationalDateTime(recovery.event_time,recovery.time_precision)}`:''}.`;
  else if(fault&&sem==='RECOVERED_CONFIRMED'&&recoveryInWindow)solutionText=`A recuperação está confirmada${semPayload.effective_from?` a partir de ${operationalDateTime(semPayload.effective_from,semPayload.effective_from_precision)}`:''}, mas o método de resolução ainda não foi informado.`;
  else if(fault&&sem==='RECOVERED_CONFIRMED'&&!recoveryInWindow)solutionText='A recuperação ocorreu fora da janela selecionada; o estado atual está recuperado, mas esta janela não contém o marco de resolução.';
  let afterText=story?.after?gatewayPhaseText(story.after):(q2?`Nas últimas 2 horas observadas: ${gatewayWindowLine(q2).replace(/<br>/g,' · ').replace(/<[^>]+>/g,'')}`:'Não há amostras posteriores suficientes para avaliar estabilidade após o evento.');
  if(afterGap!==null&&afterGap>=20)afterText+=` No recorte após a recuperação, a ocupação ficou em média ${fmt(afterGap,1)} p.p. abaixo dos Gateways comparáveis; isso merece acompanhamento, sem significar nova falha por si só.`;
  const firstActivity=story?.first_activity_after_recovery?.ts?` Primeira atividade positiva observada após a referência de recuperação: ${operationalDateTime(story.first_activity_after_recovery.ts)} (${pct(story.first_activity_after_recovery.occupancy_pct)}).`:'';
  const telemetryRecovery=story?.telemetry_recovery_at
    ?` Telemetria: primeira ocupação positiva em ${operationalDateTime(story.telemetry_recovery_at)} (${pct(story.telemetry_recovery_pct)})${story.telemetry_recovery_mode==='AFTER_GAP'?` após gap de ${fmt(story.telemetry_recovery_gap_s,0)}s; transição exata não observada`:''}.`
    :'';
  const humanRecovery=story?.recovery_boundary?` Registro humano de recuperação/intervenção: ${operationalDateTime(story.recovery_boundary)}.`:'';
  const trMs=Date.parse(story?.telemetry_recovery_at||''),hrMs=Date.parse(story?.recovery_boundary||'');
  const timingNote=Number.isFinite(trMs)&&Number.isFinite(hrMs)&&trMs<hrMs?' A telemetria já mostrava ocupação positiva antes do horário do registro humano; a associação temporal não comprova causalidade.':'';
  const nowText=`Contexto operacional: ${currentCtx}. Última evidência técnica no período: ${tech.label}.${telemetryRecovery}${humanRecovery}${timingNote}${firstActivity}`;
  const opLabel=opWindow?.label||'janela operacional';
  const opCompleteness=finiteNumber(qual.nominal_completeness_pct),opContinuity=finiteNumber(qual.continuity_coverage_pct);
  const techCompleteness=finiteNumber(techQual.nominal_completeness_pct),techContinuity=finiteNumber(techQual.continuity_coverage_pct);
  const confidence=`${periodPartial?'Cobertura operacional parcial':'Cobertura operacional completa'}: mediana da Instância ${periodCovPct===null?'N/C':fmt(periodCovPct,2)+'%'}. Este Gateway: completude nominal ${opCompleteness===null?pct(qual.coverage_pct):pct(opCompleteness)}${opContinuity!==null?`, continuidade ${pct(opContinuity)}`:''}, maior gap ${fmt(qual.largest_gap_s||0,0)}s em ${opLabel}. Rede/ICMP: completude ${techCompleteness===null?pct(techQual.coverage_pct):pct(techCompleteness)}${techContinuity!==null?`, continuidade ${pct(techContinuity)}`:''}. Intervalos sem dados não são tratados como zero.`;
  const fbList=feedback.length?`<div class="gateway-feedback-history"><h5>Informações registradas por usuários</h5>${feedback.map(r=>`<div><b>${esc(feedbackLabel(r.event_type))}</b> · ${esc(r.event_time?operationalDateTime(r.event_time,r.time_precision):operationalDateTime(r.recorded_at))} · ${esc(r.description||'')}${r.result?` <span>Resultado: ${esc(r.result)}</span>`:''}<small>${esc(r.operator||'')} · ${esc(r.source||'')}</small></div>`).join('')}</div>`:'';
  const causeMissing=hasEvent&&!cause,solutionMissing=hasEvent&&!intervention&&!recovery&&!(fault&&sem==='RECOVERED_CONFIRMED');
  const investigationBlocks=(causeMissing&&solutionMissing)?`<div class="gateway-story-unknown"><b>3–4 · Investigação e resolução</b><p>Sem causa ou resolução registradas para este evento. O TOI não inventa causalidade; registre a conclusão quando a operação confirmar o que ocorreu.</p>${gatewayHypothesesHtml()}</div>`:`<div><b>3 · Por que aconteceu</b><p>${esc(causeText)}</p>${hasEvent&&!cause?gatewayHypothesesHtml():''}</div><div><b>4 · O que foi feito / como resolveu</b><p>${esc(solutionText)}</p></div>`;
  return `<section class="gateway-story">
    <div class="gateway-story-head"><div><span>HISTÓRIA OPERACIONAL · ${esc(selectedLabel||periodLabel())}</span><h4>${esc(headline)}</h4></div><button class="gateway-feedback-open" type="button" data-entity="${esc(name)}">+ Adicionar informação operacional</button></div>
    ${gatewayEventTimelineHtml(x,story,selectedStart,selectedEnd)}
    <div class="gateway-story-timeline">
      <div><b>1 · Antes</b><p>${esc(beforeText)}</p></div>
      <div><b>2 · O que aconteceu / quando</b><p>${esc(eventText)} ${esc(duringText)}</p></div>
      ${investigationBlocks}
      <div><b>5 · Depois</b><p>${esc(afterText)}</p></div>
      <div><b>6 · Contexto e evidência técnica</b><p>${esc(nowText)}</p></div>
      <div><b>7 · Confiança da história</b><p>${esc(confidence)}</p></div>
    </div>
    ${fbList}
    <form class="gateway-feedback-form" data-entity="${esc(name)}" hidden>
      <div class="gateway-feedback-form-head"><b>Registrar conhecimento operacional</b><span>Isto não altera telemetria, diagnóstico ou ML. O registro é append-only e versionado.</span></div>
      <div class="gateway-feedback-grid">
        <label>Tipo<select name="event_type"><option value="OBSERVATION">Observação</option><option value="CAUSE">Causa</option><option value="INTERVENTION">Intervenção</option><option value="RECOVERY">Recuperação</option><option value="FALSE_POSITIVE">Falso positivo</option></select></label>
        <label>Data/hora do evento<input name="event_time" type="datetime-local"></label>
        <label>Precisão<select name="time_precision"><option value="EXACT">Exata</option><option value="APPROXIMATE">Aproximada</option><option value="UNKNOWN">Não sei</option></select></label>
        <label>Causalidade<select name="causality"><option value="UNKNOWN">Não definida</option><option value="CONFIRMED">Confirmada</option><option value="PROBABLE">Provável</option><option value="HYPOTHESIS">Hipótese</option><option value="NOT_APPLICABLE">Não se aplica</option></select></label>
        <label>Operador<input name="operator" value="TOI Admin" maxlength="200"></label>
        <label>Fonte<input name="source" placeholder="Operação / fornecedor / gestor" maxlength="300"></label>
      </div>
      <label class="gateway-feedback-wide">O que aconteceu / causa / ação<textarea name="description" required maxlength="4000" placeholder="Ex.: Serviço DS reiniciado após Gateway permanecer sem ocupação enquanto os demais estavam ativos."></textarea></label>
      <label class="gateway-feedback-wide">Resultado<input name="result" maxlength="1000" placeholder="Ex.: ocupação voltou e permaneceu estável"></label>
      <label class="gateway-feedback-wide">Observação adicional<textarea name="notes" maxlength="2000"></textarea></label>
      <div class="gateway-feedback-actions"><button type="submit">Salvar registro</button><button type="button" class="gateway-feedback-cancel">Cancelar</button><span class="gateway-feedback-status"></span></div>
    </form>
  </section>`;
}
function bindGatewayFeedbackActions(root=document){
  root.querySelectorAll('.gateway-feedback-open').forEach(btn=>{if(btn.dataset.bound)return;btn.dataset.bound='1';btn.addEventListener('click',()=>{
    const card=btn.closest('.gateway-analysis-card'),form=card?.querySelector('.gateway-feedback-form'); if(form)form.hidden=false;
  })});
  root.querySelectorAll('.gateway-feedback-cancel').forEach(btn=>{if(btn.dataset.bound)return;btn.dataset.bound='1';btn.addEventListener('click',()=>{const f=btn.closest('form');if(f)f.hidden=true})});
  root.querySelectorAll('.gateway-feedback-form').forEach(form=>{if(form.dataset.bound)return;form.dataset.bound='1';form.addEventListener('submit',async ev=>{
    ev.preventDefault(); const fd=new FormData(form),status=form.querySelector('.gateway-feedback-status'),submit=form.querySelector('button[type="submit"]');
    const payload={instancia:$('instanceSelect').value,entity:form.dataset.entity,event_type:fd.get('event_type'),event_time:fd.get('event_time')||null,time_precision:fd.get('time_precision'),causality:fd.get('causality'),operator:fd.get('operator'),source:fd.get('source'),description:fd.get('description'),result:fd.get('result'),notes:fd.get('notes')};
    try{submit.disabled=true;status.textContent='Salvando…';await api('/api/operational/feedback',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});status.textContent='Registro salvo.';await loadGatewayEntityDetail(form.closest('.gateway-analysis-card'),form.dataset.entity,true)}
    catch(e){status.textContent=`Falha: ${e.message||e}`;submit.disabled=false}
  })});
}
function gatewayEpisodeListHtml(x){
  const src=Array.isArray(x?.zero_occupancy_episodes)?x.zero_occupancy_episodes:(Array.isArray(x?.silence_episodes)?x.silence_episodes:[]);
  const all=[...src].sort((a,b)=>{
    const ta=Date.parse(a?.start||''),tb=Date.parse(b?.start||'');
    if(Number.isFinite(ta)&&Number.isFinite(tb))return ta-tb;
    return String(a?.start||'').localeCompare(String(b?.start||''));
  });
  const eps=all.length>50?all.slice(-50):all;
  if(!eps.length)return '<div class="gateway-event-list empty">Nenhum episódio de ocupação 0% foi identificado dentro da janela operacional observada.</div>';
  const shortTime=v=>{
    if(!v)return 'N/C';
    const d=new Date(v);if(Number.isNaN(d.getTime()))return operationalDateTimeExact(v);
    return `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}:${String(d.getSeconds()).padStart(2,'0')}`;
  };
  const line=e=>{
    if(e.recovery_ts)return `retomou ${shortTime(e.recovery_ts)} · ${pct(e.recovery_pct)}`;
    if(e.first_positive_after_gap_ts)return `positivo após gap ${shortTime(e.first_positive_after_gap_ts)} · ${pct(e.first_positive_after_gap_pct)} · gap ${fmt(e.recovery_gap_s,0)}s · transição exata N/C`;
    if(e.right_censored)return 'ainda aberto no fim da janela observada';
    if(e.closure_reason==='GAP')return `interrompido por gap${finiteNumber(e.recovery_gap_s)!==null?` ${fmt(e.recovery_gap_s,0)}s`:''} · retorno exato N/C`;
    return 'retorno não observado no trecho';
  };
  const label=all.length>50?`Episódios de ocupação 0% · raw observado · últimos 50 de ${n0(all.length)}`:'Episódios de ocupação 0% · raw observado · cronologia antigo → recente';
  return `<div class="gateway-event-list"><b>${label}</b>${eps.map(e=>{
    const detail=`último zero ${shortTime(e.last_zero_ts||e.end)} · ${line(e)}`;
    const full=`início ${operationalDateTimeExact(e.start)} · duração ${humanMinutes(e.duration_min)} · último zero ${operationalDateTimeExact(e.last_zero_ts||e.end)} · ${line(e)}`;
    return `<span title="${esc(full)}"><strong>${esc(operationalDateTimeExact(e.start))}</strong><i>${humanMinutes(e.duration_min)}</i><small>${esc(detail)}</small></span>`;
  }).join('')}</div>`;
}
function gatewayInvestigationRecommendations(r){
  if(!r?.valid||!r.detected)return [];
  const low=r.low_gateways||[],queues=r.congested_queues||[];
  return [{title:`Investigar distribuição de tráfego · ${low.length} gateways com baixa ocupação`,
    why:`${low.map(x=>`${esc(x.entity)}: ${pct(x.occupancy_pct)}`).join(' · ')}. Mediana dos pares ${pct(r.peer_median_pct)}. ${queues.length} fila(s) com P135 ≥ 20% no mesmo intervalo. ${r.status==='CONFIRMADO'?'Desequilíbrio confirmado por persistência.':'Condição em observação.'} Relação causal em investigação.`,
    expected:'Identificar limitações de rota ou de serviço e validar a capacidade utilizável pelas filas.',priority:r.status==='CONFIRMADO'&&queues.length?'Alta':'Média'}];
}
function gatewayInvestigationHtml(r){
  const labels={EM_INICIALIZACAO:'Aprendizado iniciando',DADOS_ATRASADOS:'Investigação suspensa · dados atrasados',
    FORA_DA_JANELA:'Observação fora da janela operacional',COMPARACAO_INSUFICIENTE:'Dados insuficientes para comparar',
    SEM_DADOS:'Aguardando leitura LIVE',SEM_DESEQUILIBRIO:'Sem desequilíbrio significativo nesta leitura',
    EM_OBSERVACAO:'Desequilíbrio em observação',CONFIRMADO:'Desequilíbrio persistente confirmado',
    NORMALIZACAO_EM_OBSERVACAO:'Distribuição recuperando · confirmação pendente',NORMALIZADO:'Distribuição normalizada'};
  r=r||{status:'EM_INICIALIZACAO'};
  const low=r.low_gateways||[],queues=r.congested_queues||[],valid=Boolean(r.valid),detected=valid&&r.detected;
  const title=labels[r.status]||r.status;
  if(!valid)return `<section class="gateway-investigation"><h4>Inteligência da distribuição · ${esc(title)}</h4><p>O TOI precisa de leituras atuais, comparáveis e dentro da janela operacional para confirmar este diagnóstico.</p></section>`;
  const summary=detected?`${low.length} de ${r.inventory_count} gateways estão com ocupação baixa enquanto ${r.loaded_gateways?.length||0} estão sob alta carga. Mediana dos pares: ${pct(r.peer_median_pct)}.`:'O TOI continuará acompanhando a distribuição e aprendendo o comportamento por faixa horária.';
  const table=low.length?`<table class="gateway-investigation-table"><thead><tr><th>Gateway</th><th>Agora</th><th>Diferença dos pares</th><th>Comportamento aprendido</th></tr></thead><tbody>${low.map(x=>`<tr><td>${esc(x.entity)}</td><td>${pct(x.occupancy_pct)}</td><td>${fmt(x.gap_pp,2)} p.p.</td><td>${x.learning_status==='APRENDIDO'?`${({QUEDA_VS_HABITUAL:'Queda em relação ao habitual',BAIXA_HABITUAL:'Baixa habitual; revisar distribuição',DESVIO_DE_BALANCEAMENTO:'Desvio do balanceamento esperado',PAPEL_A_CONFIRMAR:'Papel a confirmar'})[x.behavior]} · média histórica ${pct(x.baseline_mean_pct)}`:`${x.role_confirmed?'Balanceamento esperado · ':''}Em aprendizado · ${x.baseline_samples||0} leituras em ${x.baseline_days||0} dia(s)`}</td></tr>`).join('')}</tbody></table>`:'';
  const mapped=queues.filter(x=>(x.observed_low_gateways||[]).length||(x.observed_loaded_gateways||[]).length).length;
  const queueRows=queues.slice(0,30).map(x=>`<li><b>${esc(x.nome||x.codigo)} (${esc(x.codigo)})</b> · P135 ${pct(x.p135_pct)} · ${x.mapping_status==='USO_OBSERVADO'?`uso observado: ${esc([...(x.observed_low_gateways||[]),...(x.observed_loaded_gateways||[])].join(', ')||'outros gateways')}`:'relação com gateways ainda não comprovada'}${Object.entries(x.gateway_signals||{}).map(([name,signal])=>`<p>${esc(name)} · respostas SIP observadas: ${Object.entries(signal.sip_responses||{}).map(([code,count])=>`${esc(code)}: ${fmt(count,0)}`).join(' · ')||'N/C'} · grupos de troncos: ${esc((signal.trunk_codes||[]).join(', ')||'N/C')}</p>`).join('')}</li>`).join('');
  const detail=detected||r.status==='NORMALIZACAO_EM_OBSERVACAO'||r.status==='NORMALIZADO';
  return `<section class="gateway-investigation ${detected?'attention':''}"><h4>Inteligência da distribuição · ${esc(title)}</h4><p><b>${esc(summary)}</b></p>${table}
    ${detail?`<div class="gateway-investigation-flow">
      <div><h5>1 · Aprender</h5><p>Perfis por gateway e faixa horária, preservados entre reinicializações. Referência madura após 30 leituras em 3 dias. Amostras atrasadas e desequilíbrios não viram comportamento normal.</p><p>${Number(r.recurrence_count)||0} episódio(s) confirmado(s).</p></div>
      <div><h5>2 · Analisar</h5><p>${detected?`${Math.floor((r.observed_seconds||0)/60)} min observados · ${r.status==='CONFIRMADO'?'confirmado':'confirmação após 3 min contínuos'}. ${fmt(r.spare_channels_observed,0)} canais livres observados nos gateways de baixa ocupação; utilização depende das rotas e do papel.`:'O TOI exige 5 minutos contínuos de recuperação para encerrar o incidente.'}</p></div>
      <div><h5>3 · Investigar</h5><p>${queues.length} fila(s) congestionada(s) na mesma instância · ${mapped} com uso de gateways observado em Last Calls.</p><ul>${(r.findings||[]).map(x=>`<li>${esc(x)}</li>`).join('')}</ul><details><summary>Verificações recomendadas</summary><ul>${(r.investigation_steps||[]).map(x=>`<li>${esc(x)}</li>`).join('')}</ul></details></div>
      <div><h5>4 · Informar</h5><p>${r.status==='CONFIRMADO'?'Incidente registrado no Radar e em Alertas e Incidentes.':'Observação visível; o Radar registra o incidente após confirmar a persistência.'} O TOI apresenta coincidência temporal e evidências de uso. A causa do P135 continua em investigação.</p></div>
    </div><details class="gateway-investigation-queues"><summary>Filas observadas e limites da investigação</summary><ul>${queueRows||'<li>Sem fila com P135 ≥ 20% e amostra atual neste momento.</li>'}${queues.length>30?`<li>Exibindo 30 de ${queues.length} filas; o registro da investigação preserva todas.</li>`:''}</ul><p>${esc((r.limitations||[]).join(' '))}</p>${r.outcome?`<p>${esc(r.outcome_note)} ${r.outcome.map(x=>`${esc(x.codigo)}: P135 ${pct(x.p135_before)} → ${pct(x.p135_after)}`).join(' · ')}</p>`:''}</details>`:''}
    ${(r.excluded_gateways||[]).length?`<p>Papéis especiais respeitados: ${r.excluded_gateways.map(x=>esc(x.entity)).join(', ')}.</p>`:''}</section>`;
}
window.toiGatewayInvestigationHtml=gatewayInvestigationHtml;
function updateGatewayInvestigation(r){
  const node=$('gatewayInvestigation');if(node)node.innerHTML=gatewayInvestigationHtml(r);
  document.querySelectorAll('[data-gateway-load-signal]').forEach(node=>{
    const row=r?.valid&&r.detected?(r.low_gateways||[]).find(x=>normEntityName(x.entity)===normEntityName(node.dataset.gatewayLoadSignal)):null;
    node.textContent=row?`Baixa ocupação · ${fmt(row.gap_pp,1)} p.p. abaixo dos pares`:'';
    node.closest('.gateway-analysis-card')?.classList.toggle('gateway-load-low',Boolean(row));
  });
}
let gatewayCurrentTimer=null,gatewayCurrentView=null;
function gatewayActivityFor(name,activity){return Object.entries(activity||{}).find(([entity])=>normEntityName(entity)===normEntityName(name))?.[1]||{};}
function gatewayTrafficTimesHtml(row,snapshot,name=row?.entity,compact=false){
  const activity=gatewayActivityFor(name,snapshot?.gateway_activity),portal=activity.portal_valid||{},monitor=activity.monitor_calls||{},incident=activity.incident||{};
  const busy=finiteNumber(row?.busy),comparison=snapshot?.source_reconciliation?.rows?.find(x=>normEntityName(x.entity)===normEntityName(name));
  const calls=finiteNumber(comparison?.monitor_calls),lines=[];
  const since=portal.incident_zero_since||portal.zero_since,returned=portal.incident_returned_at||portal.returned_at;
  if(snapshot?.status==='LIVE'&&busy===0){
    lines.push(since?`Sem canais válidos ocupados desde ${operationalDateTimeExact(since)}`:'Canais válidos ocupados: início da ausência ainda não registrado');
  }else if(snapshot?.status==='LIVE'&&busy!==null&&busy>0&&returned){
    lines.push(`Canais válidos ocupados retornaram em ${operationalDateTimeExact(returned)}`);
    const previous=incident.started_at||portal.last_zero_since;
    if(!compact&&previous)lines.push(`Ausência anterior iniciada em ${operationalDateTimeExact(previous)}`);
    if(!compact&&incident.active)lines.push('Retorno em observação · aguardando confirmação de normalização por 10 min');
    else if(!compact&&incident.normalization_confirmed)lines.push(`Normalização confirmada em ${operationalDateTimeExact(incident.resolved_at)}`);
  }else if(!compact&&incident.active&&incident.started_at){
    lines.push(`Início registrado no Radar: ${operationalDateTimeExact(incident.started_at)}`);
  }
  if(!compact&&calls===0){
    lines.push(monitor.zero_since?`Monitor sem chamadas: primeira consulta com zero em ${operationalDateTimeExact(monitor.zero_since)}`:'Monitor sem chamadas: início ainda não registrado');
  }else if(!compact&&calls!==null&&calls>0&&monitor.returned_at){
    lines.push(`Monitor com chamadas: retorno observado na consulta de ${operationalDateTimeExact(monitor.returned_at)}`);
  }
  return lines.map(text=>`<small class="gateway-event-time${compact?' compact':''}">${esc(text)}</small>`).join('');
}
function gatewayCurrentHtml(row,snapshot,name=row?.entity){
  const occupancy=finiteNumber(row?.occupancy),age=snapshot?.age_s,status=row?snapshot?.status:'SEM_DADOS';
  const comparison=snapshot?.source_reconciliation?.rows?.find(x=>normEntityName(x.entity)===normEntityName(name));
  const calls=finiteNumber(comparison?.monitor_calls),idle=finiteNumber(comparison?.monitor_idle_cycles),flags=comparison?.flags||[];
  return `<b>Agora · ${occupancy===null?'N/C':pct(occupancy)}</b><small>${n0(row?.busy)} / ${n0(row?.total)} canais válidos · ${status==='LIVE'?'LIVE':status==='ATRASADO'?'leitura atrasada':'sem leitura atual'}</small><small>${snapshot?.sample_at?esc(operationalDateTimeExact(snapshot.sample_at)):'horário não disponível'}${age===null||age===undefined?'':` · há ${esc(humanAge(age))}`}</small>`+
    `<small class="gateway-monitor-calls"><b>Monitor · ${calls===null?'N/C':fmt(calls,0)} chamadas</b></small>`+
    `${idle===null?'':`<small>Monitor · ${fmt(idle,0)} ciclos ociosos</small>`}`+
    `${flags.includes('MONITOR_COM_TRAFEGO_PORTAL_ZERO')?'<small class="gateway-source-warning">Monitor informa tráfego; Portal informa zero canais válidos.</small>':''}`+
    `${flags.includes('TRAFEGO_NO_STANDBY')?'<small class="gateway-source-warning">Tráfego informado no standby. Investigar uso e horário.</small>':''}`+
    gatewayTrafficTimesHtml(row,snapshot,name);
}
function gatewaySourcesHtml(reconciliation){
  const r=reconciliation||{},monitor=r.monitor||{},rows=r.rows||[],unmatched=r.unmatched_monitor||[];
  const warnings=rows.filter(x=>(x.flags||[]).some(f=>['MONITOR_COM_TRAFEGO_PORTAL_ZERO','TRAFEGO_NO_STANDBY','NOME_SEM_CORRESPONDENCIA','NOME_AMBIGUO','COLETA_PORTAL_ATRASADA'].includes(f)));
  const notes={MONITOR_COM_TRAFEGO_PORTAL_ZERO:'Monitor informa tráfego e Portal informa zero canais válidos.',
    TRAFEGO_NO_STANDBY:'Tráfego informado no standby; verificar utilização.',
    NOME_SEM_CORRESPONDENCIA:'Nome sem correspondência exata no Monitor.',NOME_AMBIGUO:'Nome duplicado no Monitor; contador não associado.',
    COLETA_PORTAL_ATRASADA:'Coleta do Portal atrasada ou indisponível; comparar horários antes de interpretar os valores.'};
  const status={CONSULTADO:'Consulta recebida',ATRASADO:'Consulta atrasada',INDISPONIVEL:'Consulta indisponível',EM_INICIALIZACAO:'Aguardando consulta'}[monitor.status]||'Aguardando consulta';
  return `<section class="gateway-source-reconciliation ${warnings.length||unmatched.length?'attention':''}"><h4>Conferência das fontes · Portal e Monitor Orbe</h4>
    <p>${esc(r.semantics||'Canais válidos do Portal e chamadas por troncos do Monitor são contadores distintos.')}</p>
    <p><b>Monitor: ${esc(status)}</b>${monitor.consulted_at?` · consultado em ${esc(operationalDateTimeExact(monitor.consulted_at))}`:''} · hora da coleta: N/C. A hora da consulta não comprova a atualidade da amostra.</p>
    ${warnings.length?`<ul>${warnings.map(x=>`<li><b>${esc(x.entity)}</b> · ${(x.flags||[]).map(f=>notes[f]||'').filter(Boolean).map(esc).join(' ')}</li>`).join('')}</ul>`:''}
    ${unmatched.length?`<p><b>Nomes presentes apenas no Monitor:</b></p><ul>${unmatched.map(x=>`<li>${esc(x.entity)} · ${finiteNumber(x.calls)===null?'N/C':fmt(x.calls,0)} chamadas. Validar identidade do equipamento; associação automática suspensa.</li>`).join('')}</ul>`:''}</section>`;
}
function updateGatewaySources(reconciliation){const node=$('gatewaySources');if(node)node.innerHTML=gatewaySourcesHtml(reconciliation);}
function gatewayCurrentDetailHtml(name,snapshot){
  const row=(snapshot?.gateways||[]).find(x=>normEntityName(x.entity)===normEntityName(name));
  return `<section class="gateway-live-detail"><h4>Canais válidos do Portal e chamadas do Monitor</h4><div data-gateway-current="${esc(name)}">${gatewayCurrentHtml(row,snapshot,name)}</div></section>`;
}
function gatewayCurrentChip(snapshot){
  return `<span id="gatewayCurrentFreshness" class="analytics-chip ${snapshot?.status==='LIVE'?'ok':'warn'}" title="Idade da amostra do Portal. Meta da coleta: ${esc(snapshot?.target_s||'N/C')} segundos. A consulta ao Monitor aparece separadamente.">Ocupação LIVE · Portal · ${snapshot?.age_s===null||snapshot?.age_s===undefined?'sem leitura':`há ${esc(humanAge(snapshot.age_s))}`}${snapshot?.status==='ATRASADO'?' · atrasada':''}</span>`;
}
function gatewayMonitorChip(reconciliation){
  const m=reconciliation?.monitor||{},age=finiteNumber(m.consultation_age_s);
  return `<span id="gatewayMonitorFreshness" class="analytics-chip ${m.status==='CONSULTADO'?'ok':'warn'}" title="Idade da consulta HTTP ao Monitor. A fonte não informa o horário da medição das chamadas.">Monitor · ${m.status==='CONSULTADO'&&age!==null?`consultado há ${esc(humanAge(age))}`:m.status==='ATRASADO'?'consulta atrasada':'consulta indisponível'}</span>`;
}
function stopGatewayCurrentRefresh(){clearTimeout(gatewayCurrentTimer);gatewayCurrentTimer=null;gatewayCurrentView=null;}
function gatewayCurrentViewActive(view){
  return view&&gatewayCurrentView===view&&$('managerialAnalyticsModal')?.classList.contains('open')&&
    analyticsSelectionKey($('instanceSelect').value,$('queueSelect').value)===view.selection&&
    Boolean($('analyticsModalBody')?.querySelector('[data-gateway-current]'));
}
async function refreshGatewayModalCurrent(){
  const view=gatewayCurrentView;
  if(!gatewayCurrentViewActive(view)){stopGatewayCurrentRefresh();return;}
  if(view.loading)return;
  view.loading=true;
  try{
    const result=await api(`/api/infrastructure?instancia=${encodeURIComponent(view.instance)}`);
    if(!gatewayCurrentViewActive(view))return;
    const freshness=result.sources?.gateways||{};
    const snapshot={gateways:result.gateways||[],sample_at:freshness.sample_at||result.meta?.gateway_ts,
      age_s:freshness.age_s,status:freshness.status,target_s:freshness.target_s,max_age_s:freshness.max_age_s,source_reconciliation:result.source_reconciliation,gateway_activity:result.gateway_activity};
    view.snapshot=snapshot;
    view.investigation=result.gateway_investigation;
    updateGatewayInvestigation(view.investigation);
    updateGatewaySources(snapshot.source_reconciliation);
    $('analyticsModalBody').querySelectorAll('[data-gateway-current]').forEach(node=>{
      const row=snapshot.gateways.find(x=>normEntityName(x.entity)===normEntityName(node.dataset.gatewayCurrent));
      node.innerHTML=gatewayCurrentHtml(row,snapshot,node.dataset.gatewayCurrent);
    });
    const chip=$('gatewayCurrentFreshness');if(chip)chip.outerHTML=gatewayCurrentChip(snapshot);
    const monitorChip=$('gatewayMonitorFreshness');if(monitorChip)monitorChip.outerHTML=gatewayMonitorChip(snapshot.source_reconciliation);
  }catch(error){
    if(gatewayCurrentViewActive(view)){
      updateGatewayInvestigation({status:'DADOS_ATRASADOS',valid:false});
      updateGatewaySources({monitor:{status:'INDISPONIVEL'}});
      const chip=$('gatewayCurrentFreshness');if(chip){chip.className='analytics-chip warn';chip.textContent='Ocupação LIVE · atualização indisponível';}
      const monitorChip=$('gatewayMonitorFreshness');if(monitorChip)monitorChip.outerHTML=gatewayMonitorChip({monitor:{status:'INDISPONIVEL'}});
      $('analyticsModalBody').querySelectorAll('[data-gateway-current]').forEach(node=>{
        node.innerHTML='<b>Agora · N/C</b><small>Atualização LIVE indisponível</small>';
      });
    }
  }finally{view.loading=false;}
}
function scheduleGatewayCurrentRefresh(view){
  clearTimeout(gatewayCurrentTimer);
  gatewayCurrentTimer=setTimeout(async()=>{
    if(!gatewayCurrentViewActive(view)){if(gatewayCurrentView===view)stopGatewayCurrentRefresh();return;}
    if(!document.hidden)await refreshGatewayModalCurrent();
    if(gatewayCurrentViewActive(view))scheduleGatewayCurrentRefresh(view);
  },5000);
}
function refreshVisibleGatewayModal(){if(!document.hidden&&gatewayCurrentViewActive(gatewayCurrentView))refreshGatewayModalCurrent();}
document.addEventListener('visibilitychange',refreshVisibleGatewayModal);
window.addEventListener('focus',refreshVisibleGatewayModal);
async function loadGatewayEntityDetail(card,name,force=false){
  if(!card)return;
  if(card.dataset.loaded==='1'&&!force)return;
  const body=card.querySelector('.gateway-analysis-body');if(!body)return;
  card.dataset.loaded='loading';body.innerHTML='<div class="analytics-empty gateway-loading">Carregando a história deste Gateway…</div>';
  try{
    const selection=analyticsSelectionKey($('instanceSelect').value,$('queueSelect').value);
    const b=await api(gatewayAnalyticsUrl('entity',name)),sel=b?.selected_period?.analytics||{},x=(sel.entities||[])[0]||null;
    if(!card.isConnected||selection!==analyticsSelectionKey($('instanceSelect').value,$('queueSelect').value))return;
    const live=gatewayCurrentDetailHtml(name,gatewayCurrentView?.snapshot||b.current_snapshot);
    if(!sel.available||!x){body.innerHTML=live+`<div class="analytics-empty">Histórico deste Gateway indisponível: ${esc(sel.reason||'sem amostras no período')}</div>`;card.dataset.loaded='1';return}
    const key=normEntityName(name),semanticRows=b?.semantic_reference?.gateways||semanticContextsForCurrentInstance(),sem=semanticRows.find(r=>normEntityName(r.entity)===key)||gatewaySemanticFor(name)||{},h={...sem,semantic_context:sem};
    const q5=entityMap(b?.quick_windows?.['5m']?.entities||[]),q15=entityMap(b?.quick_windows?.['15m']?.entities||[]),q1=entityMap(b?.quick_windows?.['1h']?.entities||[]),q2=entityMap(b?.quick_windows?.['2h']?.entities||[]);
    const cases=(b?.operational_knowledge?.cases||[]).filter(r=>normEntityName(r.entity)===key),feedback=(b?.operational_feedback?.records||[]).filter(r=>normEntityName(r.entity)===key);
    const storyPayload=b?.operational_story_evidence||{};
    const story=Object.entries(storyPayload).find(([entity])=>normEntityName(entity)===key)?.[1]||null;
    const periodCov=b?.selected_period?.coverage_summary||{},periodPartial=periodCov.status&&periodCov.status!=='COMPLETE',periodCovPct=finiteNumber(periodCov.entity_coverage_median_pct),svc=x?.service_state_pct||{},reach=x?.reachability_pct||{},path=x?.path_pct||{},qual=x?.quality||{},techQual=x?.technical_quality||{},current=finiteNumber(x?.latest?.occupancy_pct),op=b?.operational_window||{};
    body.innerHTML=`${live}
      ${gatewayStoryHtml({name,x,h,cases,feedback,story,q2:q2.get(key),current,periodPartial,periodCovPct,selectedLabel:b.selected_period?.label||periodLabel(),selectedStart:b.selected_period?.start,selectedEnd:b.selected_period?.end,opWindow:op})}
      <div class="gateway-window-caption">Comparativos rápidos: ocupação/atividade consideram a janela operacional; Ping e caminho de rede continuam sendo evidência técnica do intervalo.</div>
      <div class="gateway-window-grid"><div><h5>Últimos 5 min</h5>${gatewayWindowLine(q5.get(key))}</div><div><h5>Últimos 15 min</h5>${gatewayWindowLine(q15.get(key))}</div><div><h5>Última 1 hora</h5>${gatewayWindowLine(q1.get(key))}</div><div><h5>Últimas 2 horas</h5>${gatewayWindowLine(q2.get(key))}</div></div>
      <div class="gateway-selected-grid">
        <div><span>Comportamento na janela operacional</span><b>Ocupação média ${pct(x?.occupancy_mean_pct)} · ${gatewayHelp('95% das leituras ficaram até','Valor abaixo do qual ficaram 95% das medições observadas dentro da janela operacional.')} ${pct(x?.occupancy_p95_pct)} · pico ${pct(x?.occupancy_peak_pct)}</b></div>
        <div><span>Atividade na janela operacional</span><b>Com atividade ${pct(svc.ACTIVE)} · atenção sem ocupação ${pct(svc.SILENT_WARN)} · crítico ${pct(svc.SILENT_CRITICAL)}</b></div>
        <div><span>Rede / Ping · 24x7</span><b>Ping OK ${pct(reach.OK)} · intermitente ${pct(reach.INTERMITTENT)} · ${gatewayHelp('caminho OK','Mostra se a rota usada pelo monitor para alcançar o Gateway estava saudável. É separado da ocupação do equipamento.')} ${pct(path.OK)}</b></div>
        <div><span>Tempo de resposta do Ping · 24x7</span><b>mediana ${fmt(x?.rtt_p50_mean_ms,1)} ms · faixa alta ${fmt(x?.rtt_p95_max_ms,1)} ms · perda média ${pct(x?.loss_mean_pct)}</b></div>
        <div><span>Períodos sem ocupação</span><b>${x?.silence_episodes_count===null||x?.silence_episodes_count===undefined?'N/C':n0(x?.silence_episodes_count)} períodos · maior ${x?.longest_silence_episode_min===null||x?.longest_silence_episode_min===undefined?'N/C':humanMinutes(x?.longest_silence_episode_min)}</b></div>
        <div><span>Qualidade operacional deste Gateway</span><b>${n0(x?.samples)} amostras · completude nominal ${pct(qual.nominal_completeness_pct??qual.coverage_pct)} · continuidade ${pct(qual.continuity_coverage_pct??qual.coverage_pct)} · maior gap ${qual.largest_gap_s===null||qual.largest_gap_s===undefined?'N/C':humanMinutes(qual.largest_gap_s/60)}</b></div>
        <div><span>Qualidade técnica · 24x7</span><b>${n0(x?.technical_samples)} amostras · completude nominal ${pct(techQual.nominal_completeness_pct??techQual.coverage_pct)} · continuidade ${pct(techQual.continuity_coverage_pct??techQual.coverage_pct)} · maior gap ${techQual.largest_gap_s===null||techQual.largest_gap_s===undefined?'N/C':humanMinutes(techQual.largest_gap_s/60)}</b></div>
        <div class="gateway-peer-evidence"><span>Comparação com outros Gateways</span><b>pares ativos ${pct(x?.peer_active_ratio_mean_pct)} · ocupação mediana dos pares ${pct(x?.peer_median_occupancy_mean_pct)} · ${gatewayHelp('maior divergência','Maior diferença observada, dentro da janela operacional, entre este Gateway e os Gateways comparáveis da mesma Instância.')} ${fmt(x?.peer_divergence_peak_pct_points,1)} p.p.</b></div>
      </div>
      ${gatewayEpisodeListHtml(x)}
      <div class="gateway-interpretation"><b>Leitura operacional TOI:</b> ${esc(gatewaySelectedInterpretation(x,h))}</div>
      ${cases.length?`<div class="gateway-knowledge"><b>Precedente conhecido:</b> ${esc(cases[0].operational_story_note||cases[0].interpretation_template||'')} <span>${String(cases[0].causality_status||'').includes('NOT_PROVEN')?'associação temporal, causa não comprovada':esc(cases[0].causality_status||'')}</span></div>`:''}
      <details class="gateway-technical-details"><summary>Ver detalhe técnico que sustenta a leitura</summary><div class="gateway-event-reason"><b>Última evidência técnica:</b> ${esc(gatewayReasonText(x))}</div></details>`;
    card.dataset.loaded='1';bindGatewayFeedbackActions(card);
  }catch(e){console.error(e);card.dataset.loaded='0';body.innerHTML=gatewayCurrentDetailHtml(name,gatewayCurrentView?.snapshot)+`<div class="analytics-empty">Falha ao carregar a história deste Gateway: ${esc(e.message||String(e))}</div>`}
}
async function renderGatewayPeriodAnalytics(){
  stopGatewayCurrentRefresh();
  const instance=$('instanceSelect').value,selection=analyticsSelectionKey(instance,$('queueSelect').value);
  const a=lastAnalytics||{};
  $('analyticsModalEyebrow').textContent='INTELIGÊNCIA · GATEWAYS · HISTÓRIA OPERACIONAL';
  $('analyticsModalTitle').textContent=`Gateways · agora e no período · Instância ${instance}`;
  $('analyticsModalSubtitle').textContent=`Período selecionado: ${a.period_label||periodLabel()} · resumo primeiro; história completa ao abrir cada equipamento.`;
  $('analyticsModalToolbar').innerHTML='<span class="analytics-chip neutral">CONSULTANDO RESUMO DOS GATEWAYS…</span>';
  $('analyticsModalBody').innerHTML='<div class="analytics-empty">Montando o resumo operacional do período…</div>';
  try{
    const b=await api(gatewayAnalyticsUrl('summary')),sel=b?.selected_period?.analytics||{},selectedMap=entityMap(sel.entities||[]),periodCov=b?.selected_period?.coverage_summary||{},op=b?.operational_window||{};
    if(selection!==analyticsSelectionKey($('instanceSelect').value,$('queueSelect').value)||!$('managerialAnalyticsModal')?.classList.contains('open'))return;
    const snapshot=b.current_snapshot||{gateways:[],status:'SEM_DADOS'},inventory=normalizeInfraRows(snapshot.gateways),currentMap=entityMap(snapshot.gateways),semanticRows=b?.semantic_reference?.gateways||semanticContextsForCurrentInstance(),semanticMap=new Map(semanticRows.map(x=>[normEntityName(x.entity),x]));
    const names=[...new Set([...(inventory||[]).map(x=>displayGatewayName(x.entity)),...(sel.entities||[]).map(x=>displayGatewayName(x.entity)),...semanticRows.map(x=>displayGatewayName(x.entity))])];
    names.sort((aa,bb)=>{const xa=selectedMap.get(normEntityName(aa)),xb=selectedMap.get(normEntityName(bb)),ha={...(semanticMap.get(normEntityName(aa))||{}),semantic_context:semanticMap.get(normEntityName(aa))||{}},hb={...(semanticMap.get(normEntityName(bb))||{}),semantic_context:semanticMap.get(normEntityName(bb))||{}};return gatewaySummaryPriority(xb,hb)-gatewaySummaryPriority(xa,ha)||aa.localeCompare(bb)});
    const shadowTone=finiteNumber(b.shadow_age_s)!==null&&finiteNumber(b.shadow_age_s)<=45?'ok':'warn',periodPartial=periodCov.status&&periodCov.status!=='COMPLETE',periodCovPct=finiteNumber(periodCov.entity_coverage_median_pct),observedGateways=(sel.entities||[]).filter(x=>(x?.samples||0)>0).length,gwAge=b.shadow_age_s===null||b.shadow_age_s===undefined?'N/C':humanAge(b.shadow_age_s);
    $('analyticsModalToolbar').innerHTML=
      gatewayCurrentChip(snapshot)+gatewayMonitorChip(snapshot.source_reconciliation)+`<button id="gatewayCurrentRefresh" class="analytics-chip" type="button">Atualizar agora</button>`+
      `<span class="analytics-chip ${shadowTone}" title="Atualização da análise técnica e do Ping; a ocupação atual usa a coleta LIVE separada.">Análise técnica · há ${gwAge}</span>`+
      `<span class="analytics-chip neutral">${esc(b.selected_period?.label||periodLabel())}</span>`+
      `<span class="analytics-chip context" title="Ocupação, atividade, ausência funcional e comparação entre Gateways são avaliadas somente neste horário. Fora dele, o TOI não conta ausência de ocupação como falha nem como zero.">Janela operacional ${esc(op.label||'N/C')}</span>`+
      `<span class="analytics-chip neutral">${inventory.length} atuais · ${observedGateways} observados</span>`+
      `<span class="analytics-chip ${periodPartial?'partial':'ok'}">COBERTURA OPERACIONAL ${periodCovPct===null?'N/C':fmt(periodCovPct,2)+'%'} ${periodPartial?'PARCIAL':'COMPLETA'}</span>`;
    const cards=names.map(name=>{
      const key=normEntityName(name),x=selectedMap.get(key)||{},sem=semanticMap.get(key)||gatewaySemanticFor(name)||{},h={...sem,semantic_context:sem},tech=gatewayTechnicalFromAnalytics(x),ctx=gatewayContextLabel(h),ctxTone=gatewayContextTone(h),peer=gatewayPeerSignal(x),cov=finiteNumber(x?.quality?.coverage_pct),current=finiteNumber(x?.latest?.occupancy_pct),when=operationalContextFor(lastData).afterHours?'fora da janela':'última leitura';
      return `<details class="gateway-analysis-card gateway-summary-card" data-entity="${esc(name)}" data-loaded="0"><summary>
        <span class="gateway-analysis-name">${esc(name)}<small data-gateway-load-signal="${esc(name)}"></small></span>
        <span class="gateway-summary-story"><b>${esc(gatewaySummaryHeadline(x,h))}</b><small>pico na janela ${pct(x?.occupancy_peak_pct)} · atividade ${pct(x?.service_state_pct?.ACTIVE)}</small></span>
        <span class="gateway-current-cell"><span data-gateway-current="${esc(name)}">${gatewayCurrentHtml(currentMap.get(key),snapshot,name)}</span><i class="gateway-health-state ${shadowTone==='ok'?tech.tone:'warn'}">${esc(shadowTone==='ok'?tech.label:'Ping sem leitura atual')}</i></span>
        <span><i class="gateway-health-state ${ctxTone}">${esc(ctx)}</i></span>
        <span><i class="gateway-health-state ${peer.tone}">${esc(peer.text)}</i></span>
        <span class="gateway-summary-cov">cobertura operacional <b>${cov===null?'N/C':fmt(cov,2)+'%'}</b></span>
      </summary><div class="gateway-analysis-body"><div class="analytics-empty">Abra este Gateway para carregar a história detalhada.</div></div></details>`;
    }).join('');
    const periodWarning=periodPartial?`<div class="analytics-period-warning"><b>Período parcial:</b> ${esc(b.selected_period?.label||periodLabel())} selecionados. Dentro da janela operacional ${esc(op.label||'N/C')}, a cobertura mediana é ${periodCovPct===null?'N/C':fmt(periodCovPct,2)+'%'}. Fora da janela operacional, ausência de ocupação não entra na história; Ping, perda e caminho de rede continuam sendo acompanhados como evidência técnica 24x7.</div>`:'';
    const historyWarning=!sel.available?`<div class="analytics-period-warning">Histórico dos Gateways indisponível para o período selecionado. Os contadores atuais continuam sendo consultados pelas fontes do Portal e do Monitor.</div>`:'';
    $('analyticsModalBody').innerHTML=`<div id="gatewayInvestigation"></div><div id="gatewaySources"></div>${historyWarning}${periodWarning}<section class="analytics-explain gateway-scope-explain"><h4>Como ler</h4><p><b>Agora</b> mostra ocupação e canais válidos do Portal. <b>Monitor</b> mostra chamadas por troncos do Monitor Orbe, com consulta independente e horário de coleta não informado pela fonte. O TOI atualiza ambos enquanto esta janela estiver aberta. A história e o Ping usam as amostras do período selecionado.</p></section><section class="gateway-analysis-list">${cards||'<div class="analytics-empty">Sem Gateways no intervalo.</div>'}</section>`;
    updateGatewayInvestigation(b.gateway_investigation);
    updateGatewaySources(snapshot.source_reconciliation);
    document.querySelectorAll('.gateway-analysis-card').forEach(card=>card.addEventListener('toggle',()=>{if(card.open&&card.dataset.loaded!=='1'&&card.dataset.loaded!=='loading')loadGatewayEntityDetail(card,card.dataset.entity)}));
    gatewayCurrentView={instance,selection,snapshot,investigation:b.gateway_investigation,loading:false};
    $('gatewayCurrentRefresh').addEventListener('click',refreshGatewayModalCurrent);
    scheduleGatewayCurrentRefresh(gatewayCurrentView);
  }catch(e){console.error(e);$('analyticsModalToolbar').innerHTML='<span class="analytics-chip bad">HISTÓRICO DOS GATEWAYS INDISPONÍVEL</span>';$('analyticsModalBody').innerHTML=`<div class="analytics-empty">Falha na consulta histórica: ${esc(e.message||String(e))}</div>`}
}
async function renderTrunkPeriodAnalytics(){
  const a=lastAnalytics||{},ia=a.instance_analytics||{},rows=[...(ia.trunk_group_profiles||[])];
  const qProfiles=((a.capacity_analytics||{}).operational||{}).trunk_group_profiles||[];
  const related=new Set(qProfiles.map(x=>String(x.entity||'')));
  const qs=a.queue_summary||{},inst=a.instancia||$('instanceSelect').value,code=$('queueSelect').value;
  const currentTrunks=normalizeInfraRows((lastData?.infra_all||lastData?.infra||{}).trunks||[]);
  const observedTrunks=rows.filter(x=>(x?.samples||0)>0).length;
  const trunkCov=a?.coverage?.trunk||{},trunkCovPct=finiteNumber(trunkCov.data_coverage_pct??trunkCov.coverage_pct);
  const trunkPartial=!trunkCov.complete;
  $('analyticsModalEyebrow').textContent='INTELIGÊNCIA · GRUPOS DE TRONCOS · PRESSÃO TEMPORAL';
  $('analyticsModalTitle').textContent=`Grupos de Troncos · comportamento temporal · Instância ${inst}`;
  $('analyticsModalSubtitle').textContent=`${a.period_label||periodLabel()} · Instância completa + grupos relacionados à fila por Last Calls no próprio período.`;
  $('analyticsModalToolbar').innerHTML=`<span class="analytics-chip neutral">${currentTrunks.length} atuais</span><span class="analytics-chip neutral">${observedTrunks} observados</span><span class="analytics-chip context">INSTÂNCIA COMPLETA</span><span class="analytics-chip neutral">${related.size} relacionados à fila</span><span class="analytics-chip ${trunkPartial?'partial':'ok'}">COBERTURA ${trunkCovPct===null?'N/C':fmt(trunkCovPct,2)+'%'} ${trunkPartial?'PARCIAL':'COMPLETA'}</span><span class="analytics-chip neutral">carregando 5m/15m/1h/2h...</span>`;
  let quick={};
  try{
    const periods=['5m','15m','1h','2h'];
    const vals=await Promise.all(periods.map(p=>api(analyticsUrlForPeriod(inst,code,p))));
    periods.forEach((p,i)=>quick[p]=new Map((((vals[i]?.instance_analytics)||{}).trunk_group_profiles||[]).map(x=>[String(x.entity||''),x])));
    $('analyticsModalToolbar').innerHTML=`<span class="analytics-chip neutral">${currentTrunks.length} atuais</span><span class="analytics-chip neutral">${observedTrunks} observados</span><span class="analytics-chip context">INSTÂNCIA COMPLETA</span><span class="analytics-chip neutral">${related.size} relacionados à fila</span><span class="analytics-chip ${trunkPartial?'partial':'ok'}">COBERTURA ${trunkCovPct===null?'N/C':fmt(trunkCovPct,2)+'%'} ${trunkPartial?'PARCIAL':'COMPLETA'}</span><span class="analytics-chip ok">5m / 15m / 1h / 2h</span>`;
  }catch(e){console.warn('Comparativos rápidos de troncos indisponíveis',e);quick={}}
  rows.sort((x,y)=>(finiteNumber(y.p95_pct)??-1)-(finiteNumber(x.p95_pct)??-1));
  const maxP95=rows.find(x=>finiteNumber(x.p95_pct)!==null)||{};
  const persistent=rows.filter(x=>(finiteNumber(x.minutes_over_85)||0)>=15).length;
  const pressureNow=rows.filter(x=>(finiteNumber(x.current_pct)||0)>=80).length;
  const cards=`<div class="analytics-stat-grid trunk-exec-stats">
    <div class="analytics-stat"><span>Maior valor que 95% das leituras não ultrapassaram</span><b>${pct(maxP95.p95_pct)}</b><small>${esc(maxP95.entity||'—')}</small></div>
    <div class="analytics-stat"><span>Pressão agora ≥80%</span><b>${n0(pressureNow)}</b><small>grupos na fotografia atual</small></div>
    <div class="analytics-stat"><span>Persistentes observados ≥85%</span><b>${n0(persistent)}</b><small>≥15 min no período</small></div>
    <div class="analytics-stat"><span>Demanda observada da fila</span><b>${n0(qs?.calls_dialing?.mean)}</b><small>média discando · P34 ${pct(qs?.p34_pct?.mean)} · P135 ${pct(qs?.p135_pct?.mean)}</small></div>
  </div>`;
  const qline=(entity,p)=>{const x=quick?.[p]?.get(String(entity||''));return x?`méd ${pct(x.mean_pct)} · 95% das leituras ficaram até ${pct(x.p95_pct)} · pico ${pct(x.peak_pct)}`:'N/C'};
  const body=rows.map((x,i)=>{
    const rel=related.has(String(x.entity||''));
    const q=x.quality||{};
    const trend=String(x.trend_15m_vs_prev45m||'N/C');
    const activity=finiteNumber(x.active_samples_pct);
    const cov=finiteNumber(q.coverage_pct);
    const baseInterp=(finiteNumber(x.minutes_over_85)||0)>=15?'PRESSÃO PERSISTENTE':(finiteNumber(x.peak_pct)||0)>=85?'PICO TRANSITÓRIO':'SEM PRESSÃO SUSTENTADA';
    const interp=cov!==null&&cov<20?'EVIDÊNCIA INSUFICIENTE':(cov!==null&&cov<70?`${baseInterp} · COBERTURA PARCIAL`:baseInterp);
    return `<details class="trunk-analysis-card" ${i===0?'open':''}>
      <summary><span><b>${esc(x.entity)}</b>${rel?'<i>FILA</i>':''}</span><span class="occ ${trunkPressureClass(x.current_pct)}">${pct(x.current_pct)}</span><span>95% das leituras ficaram até ${pct(x.p95_pct)}</span><span>${esc(trend)}</span><span>${esc(interp)}</span></summary>
      <div class="trunk-analysis-body">
        <div class="trunk-quick-grid">
          <div><span>5 min</span><b>${qline(x.entity,'5m')}</b></div><div><span>15 min</span><b>${qline(x.entity,'15m')}</b></div><div><span>1 hora</span><b>${qline(x.entity,'1h')}</b></div><div><span>2 horas</span><b>${qline(x.entity,'2h')}</b></div>
        </div>
        <div class="trunk-window-grid">
          <div><span>Período selecionado ${cov!==null&&cov<95?'· observado/parcial':'· completo'}</span><b>Média observada ${pct(x.mean_pct)} · valor que 95% das leituras não ultrapassaram ${pct(x.p95_pct)} · Pico observado ${pct(x.peak_pct)}</b></div>
          <div><span>Folga no nível que 95% das leituras não ultrapassaram</span><b>${pct(x.headroom_p95_pct)} · atual ${n0(x.busy)} / ${n0(x.total)}</b></div>
          <div><span>Persistência</span><b>≥75% ${humanMinutes(x.minutes_over_75)} · ≥85% ${humanMinutes(x.minutes_over_85)} · ≥90% ${humanMinutes(x.minutes_over_90)}</b></div>
          <div><span>Episódios</span><b>≥85% ${n0(x.episodes_over_85)} · maior ${humanMinutes(x.longest_episode_over_85_min)}</b></div>
          <div><span>Atividade</span><b>com atividade ${pct(activity)} · zerado ${pct(x.zero_samples_pct)}</b></div>
          <div><span>Tendência 15m × 45m anteriores</span><b>${esc(trend)}${finiteNumber(x.trend_delta_pct_points)!==null?` · ${fmt(x.trend_delta_pct_points,1)} p.p.`:''}</b></div>
          <div><span>Qualidade</span><b>${n0(x.samples)} amostras · cobertura ${pct(q.coverage_pct)} · gap máx ${humanMinutes(q.largest_gap_min)}</b></div>
          <div><span>Escopo</span><b>${rel?'Relacionado à fila por Last Calls no período':'Contexto da Instância; não mapeado à fila no período'}</b></div>
        </div>
        <div class="trunk-interpretation"><b>Interpretação executiva:</b> ${interp}. Pressão é avaliada separadamente de atividade; coincidência com P34/P135/Hit Rate é evidência contextual e não causalidade automática.</div>
      </div>
    </details>`;
  }).join('');
  const periodWarning=trunkPartial?`<div class="analytics-period-warning"><b>Período parcial:</b> ${a.period_label||periodLabel()} solicitados; cobertura de infraestrutura ${trunkCovPct===null?'N/C':fmt(trunkCovPct,2)+'%'}. Estatísticas e persistência representam somente os trechos observados; ausência de dados não é ocupação zero.</div>`:'';
  $('analyticsModalBody').innerHTML=`${periodWarning}${cards}<section class="analytics-explain"><h4>Como ler esta análise</h4><p><b>Pressão</b> = intensidade/persistência de ocupação. <b>Atividade</b> = existência de tráfego. Um grupo pode estar verde em atividade e, ao mesmo tempo, em atenção por pressão. FILA e INSTÂNCIA permanecem escopos distintos.</p></section><section class="trunk-analysis-list">${body||'<div class="analytics-empty">Sem histórico individual por grupo.</div>'}</section>`;
}
async function openEntityPeriodAnalytics(kind){
  if(!lastAnalytics){console.warn('Analytics ainda não disponível.');return}
  const m=$('managerialAnalyticsModal');m.classList.add('open');m.setAttribute('aria-hidden','false');document.body.classList.add('analytics-modal-open');
  setTimeout(()=>$('managerialAnalyticsClose')?.focus(),0);
  if(kind==='gateways')await renderGatewayPeriodAnalytics();else await renderTrunkPeriodAnalytics();
}

function renderOperationalPressureManagerial(a){
  const po=a?.operational_pressure||{},r=po.relations||{},w=po.worst_observed||{},cov=finiteNumber(po.evidence_coverage_pct);
  const breakdown=po.overall_breakdown||{};
  const entity=`QUEUE:${a?.instancia||$('instanceSelect').value}:${a?.codigo||$('queueSelect').value}`;
  const feedback=a?.operational_feedback?.queue?.records||[];
  $("analyticsModalEyebrow").textContent='INTELIGÊNCIA · PRESSÃO OPERACIONAL';
  $("analyticsModalTitle").textContent='Pressão Operacional · esforço observado para sustentar a fila';
  $("analyticsModalSubtitle").textContent=`${a?.period_label||periodLabel()} · leitura do período selecionado · explicável e ainda não ML.`;
  $("analyticsModalToolbar").innerHTML=`<span class="analytics-chip ${pressureTone(po.level)}">${finiteNumber(po.score)===null?'N/C':fmt(po.score,0)+'/100'} · ${esc(po.level||'N/C')}</span><span class="analytics-chip neutral">cobertura fontes ${cov===null?'N/C':fmt(cov,2)+'%'}</span>`;
  const contrib=(po.contributors||[]).map(x=>{
    if(x.scored===false)return `<div class="pressure-driver ${pressureTone(x.level)}"><span>${esc(x.name)}</span><b>${esc(String(x.state||'CONTEXTO'))}</b><small>${esc(x.note||'contexto não atribuído automaticamente à fila')}</small></div>`;
    const d=x.detail||{},c=d.score_components||{},meaning=pressureIndexMeaning(x.score);
    const detail=x.metric==='p34'||x.metric==='p135'
      ?`Em 95% das leituras, o congestionamento ficou em até ${pct(d.p95)} · pico ${pct(d.peak)} · ≥20% ${humanMinutes(d.minutes_over_20)} · persistência ${pct(d.persistence_pct_observed)}`
      :`Troncos: ${d.trunk_p95===null||d.trunk_p95===undefined?'N/C':pct(d.trunk_p95)}${d.trunk_entity?' · '+esc(d.trunk_entity):''}. Gateways: ${d.gateway_p95===null||d.gateway_p95===undefined?'N/C':pct(d.gateway_p95)}${d.gateway_entity?' · '+esc(displayGatewayName(d.gateway_entity)):''}. Prevaleceu ${d.selected_source==='GATEWAY'?'o Gateway':'o grupo de troncos'}${d.gateway_exclusion?' · '+esc(d.gateway_exclusion):''}.`;
    const interpretation=(d.interpretation_text||meaning.text);
    const interpretationLabel=(d.interpretation_label||meaning.label);
    return `<div class="pressure-driver ${pressureTone(interpretationLabel)}" title="${esc(d.score_explanation||detail)}"><span>${esc(x.name)}</span><b>Índice ${fmt(x.score,0)}/100 · ${esc(interpretationLabel)}</b><small><b>O que significa:</b> ${esc(interpretation)}</small><small>${detail} · peso ${fmt((Number(x.weight)||0)*100,2)}%</small><small>Faixas do índice: 0–44 baixa · 45–69 moderada · 70–84 elevada · 85–100 crítica.</small></div>`;
  }).join('');
  const hypotheses=(po.hypotheses||[]).map((x,i)=>`<div class="operational-feedback-row"><b>${i+1}. ${esc(x.name)}</b> · força ${esc(x.strength||'N/C')} · ${esc(x.why||'')}</div>`).join('')||'<div class="analytics-empty">Nenhuma hipótese dominante sustentada pelos dados desta janela.</div>';
  const fav=(po.evidence_for||[]).map(x=>`<li>${esc(x)}</li>`).join('')||'<li>Sem evidência adicional forte.</li>';
  const contra=(po.evidence_against||[]).map(x=>`<li>${esc(x)}</li>`).join('')||'<li>Sem fator de descarte adicional.</li>';
  const recs=(po.recommendations||[]).map((x,i)=>`<div class="operational-feedback-row"><b>${i+1}. ${esc(x.title)}</b> · ${esc(x.why)} <span class="semantic-note">Impacto esperado: ${esc(x.expected||'')}</span></div>`).join('');
  const feedbackHtml=feedback.length?`<section class="analytics-section"><h4>Conhecimento informado pela operação</h4>${feedback.map(x=>`<div class="operational-feedback-row"><b>${esc(feedbackLabel(x.event_type))}</b> · ${esc(x.event_time?operationalDateTime(x.event_time,x.time_precision):operationalDateTime(x.recorded_at))} · ${esc(x.description||'')}</div>`).join('')}</section>`:'';
  $("analyticsModalBody").innerHTML=`<section class="operational-story-banner"><b>Leitura executiva:</b> ${esc(po.executive_summary||`Pressão ${String(po.level||'N/C').toLowerCase()} no período.`)}<br><b>Por que ${finiteNumber(po.score)===null?'N/C':fmt(po.score,1)+'/100'}?</b> O índice considera 55% do maior sinal (${finiteNumber(breakdown.highest_dimension)===null?'N/C':fmt(breakdown.highest_dimension,1)+'/100'}) e 45% da média ponderada de P34, P135 e pressão Telecom (${finiteNumber(breakdown.weighted_mean)===null?'N/C':fmt(breakdown.weighted_mean,1)+'/100'}). Telecom usa a maior ocupação válida entre troncos e Gateways relacionados, sem somá-los. Ele resume a pressão observada no período; não representa percentual de congestionamento atual.</section>
    <section class="pressure-drivers"><h4>O que pressionou a fila no período</h4><div class="pressure-driver-grid">${contrib||'<div class="analytics-empty">Evidência insuficiente para calcular contribuições.</div>'}</div></section>
    ${w.ts?`<section class="event-story"><h4>Pior instante observado</h4><p><b>${eventStamp(w.ts)}</b> · sintoma dominante ${esc(w.dominant_metric||'N/C')} · P34 ${pct(w.p34_pct)} · P135 ${pct(w.p135_pct)} · discando ${n0(w.calls_dialing)} · Hit Rate 15' ${pct(w.hit_rate15)} · buffer ${n0(w.buffer)} · agentes disponíveis ${n0(w.agents_available)}.</p></section>`:''}
    <section class="analytics-section"><h4>Hipóteses de causa · não causalidade comprovada</h4>${hypotheses}</section>
    <section class="analytics-section"><h4>Evidências a favor / contra</h4><div class="event-story-grid"><div><b>A favor</b><ul>${fav}</ul></div><div><b>Contra / enfraquecem hipóteses</b><ul>${contra}</ul></div></div></section>
    <section class="analytics-section"><h4>Recomendações operacionais</h4>${recs||'<div class="analytics-empty">Sem recomendação específica nesta janela.</div>'}</section>
    <section class="analytics-section"><h4>Relações operacionais de contexto</h4><div class="analytics-facts"><span>Discando médio <b>${r.calls_dialing_mean===null||r.calls_dialing_mean===undefined?'N/C':n0(r.calls_dialing_mean)}</b></span><span>Pico discando <b>${r.calls_dialing_peak===null||r.calls_dialing_peak===undefined?'N/C':n0(r.calls_dialing_peak)}</b></span><span>Agentes disponíveis médios <b>${r.agents_available_mean===null||r.agents_available_mean===undefined?'N/C':fmt(r.agents_available_mean,1)}</b></span><span>Discando/agente disponível <b>${r.dialing_per_available_agent===null||r.dialing_per_available_agent===undefined?'N/C':fmt(r.dialing_per_available_agent,1)}</b></span><span>Buffer/agente disponível <b>${r.buffer_per_available_agent===null||r.buffer_per_available_agent===undefined?'N/C':fmt(r.buffer_per_available_agent,1)}</b></span><span>Hit Rate 15' médio <b>${r.hit_rate15_mean===null||r.hit_rate15_mean===undefined?'N/C':pct(r.hit_rate15_mean)}</b></span></div><p class="semantic-note">${esc(r.attendance_note||'')}</p></section>
    <section class="analytics-explain"><h4>Como interpretar os valores /100</h4><p><b>Leitura do período:</b> quando mostramos que 95% das leituras ficaram em até um valor, isso significa que somente 5% ficaram acima dele. O pico aparece separado.</p><p><b>Os índices /100 não são percentuais de P34/P135 nem probabilidades.</b> Eles traduzem a severidade observada na janela selecionada.</p><p><b>Leitura direta:</b> 0–44 = BAIXA; 45–69 = MODERADA; 70–84 = ELEVADA; 85–100 = CRÍTICA.</p><p><b>Exemplos:</b> 23/100 = pressão baixa, com sinal limitado; 70/100 = pressão elevada e relevante; 100/100 = teto da escala de pressão da dimensão, sem significar P34/P135 de 100%.</p><p>${esc(po?.score_method?.dimension_method||'P34/P135 combinam o valor que 95% das leituras não ultrapassaram, o pico e o tempo acima do limite crítico.')}</p><p>${esc(po?.score_method?.overall_method||'A Pressão Operacional consolida as dimensões disponíveis sem deixar um gargalo severo desaparecer na média.')}</p><p><b>Importante:</b> P34 = congestionamento na companhia/caminho de saída; P135 = congestionamento interno. Eles não são combinados sob o mesmo nome e nenhuma coincidência temporal é tratada como causa comprovada.</p></section>
    <section class="analytics-explain"><h4>Metodologia observacional</h4><p>${esc(po.methodology||'')}</p></section>
    ${feedbackHtml}${genericFeedbackForm(entity,'Ex.: fila operou com poucos agentes disponíveis; campanha específica elevou a discagem; ajuste operacional realizado.')}`;
  bindGenericFeedbackActions(async()=>{lastAnalytics=await api(analyticsUrl());renderOperationalPressureManagerial(lastAnalytics)});
}
function openManagerialAnalytics(kind){
  if(!lastAnalytics){
    console.warn("Analytics ainda não disponível.");
    return;
  }
  if(kind==="stuck"){
    renderStuckManagerial(lastAnalytics);
    $("navStuckAnalytics")?.classList.add("analytics-selected");
  }else if(kind==="pressure"){
    renderOperationalPressureManagerial(lastAnalytics);
  }else{
    renderCapacityManagerial(lastAnalytics);
    $("navCapacityAnalytics")?.classList.add("analytics-selected");
  }
  const m=$("managerialAnalyticsModal");m.classList.add("open");m.setAttribute("aria-hidden","false");
  document.body.classList.add("analytics-modal-open");
  setTimeout(()=>$("managerialAnalyticsClose")?.focus(),0);
}

function incidentHistoryMetricLabel(type){
  const t=String(type||'').toUpperCase();
  if(t==='P34')return 'Congestionamento na companhia · P34';
  if(t==='P135')return 'Congestionamento interno · P135';
  return t||'Incidente observado';
}
function incidentHistoryRows(a,d){
  const rows=reconcileRecentIncidents(a,d?.recent_incidents||[]),bs=Date.parse(a?.bounds?.start||''),be=Date.parse(a?.bounds?.end||'');
  return rows.filter(x=>{
    const st=Date.parse(x?.start||''),en=Date.parse(x?.resolved_at||x?.end||a?.bounds?.end||'');
    if(!Number.isFinite(st))return false;
    if(Number.isFinite(bs)&&Number.isFinite(en)&&en<bs)return false;
    if(Number.isFinite(be)&&st>be)return false;
    return true;
  }).sort((x,y)=>{
    const xt=Date.parse((String(x?.status||'').toUpperCase()==='ATIVO'?x?.start:(x?.resolved_at||x?.end||x?.start))||'');
    const yt=Date.parse((String(y?.status||'').toUpperCase()==='ATIVO'?y?.start:(y?.resolved_at||y?.end||y?.start))||'');
    return (Number.isFinite(yt)?yt:0)-(Number.isFinite(xt)?xt:0);
  });
}
function nearestSeriesPoint(series,ts){
  const t=Date.parse(ts||'');if(!Number.isFinite(t)||!series?.length)return null;
  let best=null,dist=Infinity;
  for(const p of series){const n=Date.parse(p?.ts||'');if(!Number.isFinite(n))continue;const d=Math.abs(n-t);if(d<dist){dist=d;best=p}}
  return dist<=120000?best:null;
}
function incidentHistoryContext(a,inc){
  const qs=a?.series?.queue||[],ts=a?.series?.trunk||[],st=Date.parse(inc?.start||''),en=Date.parse(inc?.resolved_at||inc?.end||a?.bounds?.end||qs.at?.(-1)?.ts||''),metric=String(inc?.type||'').toLowerCase();
  const inRange=qs.filter(p=>{const t=Date.parse(p?.ts||'');return Number.isFinite(t)&&(!Number.isFinite(st)||t>=st)&&(!Number.isFinite(en)||t<=en)});
  const peakTs=inc?.max_ts||inc?.start,peak=nearestSeriesPoint(inRange.length?inRange:qs,peakTs);
  const otherKey=metric==='p34'?'p135_pct':'p34_pct';
  const otherVals=inRange.map(p=>finiteNumber(p?.[otherKey])).filter(v=>v!==null),otherMax=otherVals.length?Math.max(...otherVals):null;
  const trunkVals=ts.filter(p=>{const t=Date.parse(p?.ts||'');return Number.isFinite(t)&&(!Number.isFinite(st)||t>=st)&&(!Number.isFinite(en)||t<=en)&&finiteNumber(p?.occupancy)!==null});
  const trunkPeak=trunkVals.length?trunkVals.reduce((m,p)=>(finiteNumber(p.occupancy)||0)>(finiteNumber(m.occupancy)||0)?p:m,trunkVals[0]):null;
  return {peak,otherMax,trunkPeak,samples:inRange.length};
}
function incidentHistoryNarrative(a,inc,ctx){
  const type=String(inc?.type||'').toUpperCase(),resolved=String(inc?.status||'').toUpperCase()==='RESOLVIDO',bs=Date.parse(a?.bounds?.start||''),st=Date.parse(inc?.start||''),startedBefore=Number.isFinite(bs)&&Number.isFinite(st)&&st<bs;
  const subject=type==='P34'?'O congestionamento na companhia/caminho de saída':type==='P135'?'O congestionamento interno':`O evento ${type}`;
  const startText=startedBefore?`já estava em faixa crítica no início do período; o início conhecido é ${eventStamp(inc.start)}`:`entrou em faixa crítica (≥20%) em ${eventStamp(inc.start)}`;
  const peak=finiteNumber(inc?.max),peakText=peak!==null?` A maior leitura observada foi ${pct(peak)}${inc?.max_ts?` às ${eventStamp(inc.max_ts)}`:''}.`:'';
  const endText=resolved&&inc?.resolved_at?` A primeira amostra abaixo de 20% foi observada em ${eventStamp(inc.resolved_at)}, encerrando o episódio no TOI.`:` O episódio permaneceu aberto no fim do recorte observado; o TOI não inventa normalização sem evidência.`;
  const other=ctx?.otherMax;
  let relation='';
  if(other!==null){const otherLabel=type==='P34'?'P135':'P34';relation=other>=20?` Houve sobreposição temporal com ${otherLabel}, que chegou a ${pct(other)}; essa coincidência não comprova causalidade.`:` ${otherLabel} permaneceu abaixo do limiar crítico no episódio (máx. ${pct(other)}).`}
  return `${subject} ${startText}.${peakText}${endText}${relation}`.replace(/\.\s*\./g,'.');
}
function renderIncidentHistory(a,d){
  const rows=incidentHistoryRows(a,d),resolved=rows.filter(x=>String(x?.status||'').toUpperCase()==='RESOLVIDO').length,active=rows.length-resolved,cov=periodCoveragePct(a);
  const monitorById=new Map((window.toiSbcMonitorFeed?.incidentes||[]).filter(q=>q.id).map(q=>[q.id,q]));
  const sbcHistory=[...monitorById.values()].map(q=>`<div class="recent-row" data-event-id="${esc(q.id)}"><span>${esc(eventStamp(q.confirmed_at))}</span><span class="chip ${q.status==='Ativo'?'active':'resolved'}">${esc(q.status)}</span><span>${esc(q.type)} · ${esc(q.payload?.peer||q.payload?.agent||q.payload?.par||q.payload?.ip||q.entity)} · início ${esc(eventStamp(q.started_at))}</span></div>`).join('');
  const sbcSection=`<section class="sbc-history"><h3>Monitor_alertas · global</h3><p>Eventos após 5 minutos contínuos; saúde do peer HA não verificada por heartbeat.</p>${sbcHistory||'<p>Nenhum incidente confirmado.</p>'}</section>`;
  $("analyticsModalEyebrow").textContent='INTELIGÊNCIA · INCIDENTES · LIFECYCLE OPERACIONAL';
  $("analyticsModalTitle").textContent=`Incidentes · o que aconteceu no período · Instância ${a?.instancia||$("instanceSelect")?.value||'—'}`;
  $("analyticsModalSubtitle").textContent=`Fila ${$("queueSelect")?.selectedOptions?.[0]?.textContent||'—'} · ${a?.period_label||periodLabel()} · um episódio é uma unidade de lifecycle, não uma coleção de marcadores.`;
  $("analyticsModalToolbar").innerHTML=`<span class="analytics-chip neutral">${esc(a?.period_label||periodLabel())}</span><span class="analytics-chip ${active?'bad':'ok'}">${active} ativo${active===1?'':'s'}</span><span class="analytics-chip ok">${resolved} resolvido${resolved===1?'':'s'}</span><span class="analytics-chip ${periodIsPartial(a)?'partial':'ok'}">COBERTURA ${fmt(cov,2)}% ${periodIsPartial(a)?'PARCIAL':'COMPLETA'}</span>`;
  if(!rows.length){$("analyticsModalBody").innerHTML='<div class="incident-history-empty">Nenhum episódio P34/P135 ≥20% intersectou o período selecionado.</div>'+sbcSection;return}
  const latest=a?.series?.queue?.at?.(-1)||d?.latest||{},queueSeries=a?.series?.queue||[];
  const body=rows.map((inc,i)=>{
    const type=String(inc?.type||'').toUpperCase(),isResolved=String(inc?.status||'').toUpperCase()==='RESOLVIDO',ctx=incidentHistoryContext(a,inc),peak=ctx.peak||{},eventEnd=inc?.resolved_at||inc?.end||a?.bounds?.end||latest?.ts;
    const dur=duration(inc?.start,eventEnd),endTitle=isResolved?'Normalização observada':'Fim do recorte',endMain=isResolved&&inc?.resolved_at?eventStamp(inc.resolved_at):'Permaneceu aberto',endSub=isResolved?'primeira amostra <20%':'sem normalização comprovada';
    const p34=finiteNumber(peak?.p34_pct),p135=finiteNumber(peak?.p135_pct),calls=finiteNumber(peak?.calls_dialing),hit=finiteNumber(peak?.hit_rate15),buf=finiteNumber(peak?.buffer),agents=finiteNumber(peak?.agents_available),trunk=finiteNumber(ctx?.trunkPeak?.occupancy);
    const causal=`Causalidade: não comprovada automaticamente. O TOI preserva correlações temporais como contexto e não converte coincidência em causa.`;
    return `<article class="incident-history-card ${isResolved?'resolved':'active'}">
      <header class="incident-history-head"><div><h4>${i+1}. ${esc(incidentHistoryMetricLabel(type))}</h4><p>${eventStamp(inc?.start)} → ${isResolved&&inc?.resolved_at?eventStamp(inc.resolved_at):'aberto no fim da janela'} · duração observada ${esc(dur)}</p></div><span class="incident-history-status ${isResolved?'resolved':'active'}">${isResolved?'RESOLVIDO':'ATIVO / ABERTO'}</span></header>
      <div class="incident-history-rail"><div class="incident-history-step"><span>Início crítico</span><b>${eventStamp(inc?.start)}</b><small>limiar ≥20%</small></div><div class="incident-history-step"><span>Pico observado</span><b>${finiteNumber(inc?.max)===null?'N/C':pct(inc.max)}</b><small>${inc?.max_ts?eventStamp(inc.max_ts):'horário não disponível'}</small></div><div class="incident-history-step"><span>${endTitle}</span><b>${endMain}</b><small>${endSub}</small></div></div>
      <div class="incident-history-body"><p><b>Narrativa:</b> ${esc(incidentHistoryNarrative(a,inc,ctx))}</p>
        <div class="incident-history-facts"><div class="incident-history-fact"><span>No pico · P34</span><b>${p34===null?'N/C':pct(p34)}</b></div><div class="incident-history-fact"><span>No pico · P135</span><b>${p135===null?'N/C':pct(p135)}</b></div><div class="incident-history-fact"><span>Discando</span><b>${calls===null?'N/C':n0(calls)}</b></div><div class="incident-history-fact"><span>Hit Rate 15'</span><b>${hit===null?'N/C':pct(hit)}</b></div><div class="incident-history-fact"><span>Buffer</span><b>${buf===null?'N/C':n0(buf)}</b></div><div class="incident-history-fact"><span>Agentes disponíveis</span><b>${agents===null?'N/C':fmt(agents,1)}</b></div><div class="incident-history-fact"><span>Troncos no episódio</span><b>${trunk===null?'N/C':`pico ${pct(trunk)}`}</b></div><div class="incident-history-fact"><span>Amostras da fila no episódio</span><b>${n0(ctx.samples)}</b></div></div>
        <div class="incident-history-note">${esc(causal)}</div>
      </div></article>`;
  }).join('');
  $("analyticsModalBody").innerHTML=`<section class="incident-history-intro"><b>Leitura do período:</b> ${rows.length} episódio${rows.length===1?'':'s'} de congestionamento crítico reconciliado${rows.length===1?'':'s'} pela mesma fonte temporal usada em DESTAQUES DO PERÍODO e INCIDENTES RECENTES. Mais recente primeiro; dentro de cada card, início → pico → normalização/fim.</section><div class="incident-history-list">${body}</div>${sbcSection}`;
}
function openIncidentHistory(){
  if(!lastAnalytics){console.warn('Histórico de incidentes: analytics ainda não disponível.');return}
  renderIncidentHistory(lastAnalytics,lastData||{});
  const m=$("managerialAnalyticsModal");m.classList.add("open");m.setAttribute("aria-hidden","false");document.body.classList.add("analytics-modal-open");setTimeout(()=>$("managerialAnalyticsClose")?.focus(),0);
}

function reconcileRecentIncidents(a,fallback=[]){
  const out=[],cg=a?.congestion_analytics||{};
  for(const [metric,type] of [['p34','P34'],['p135','P135']]){
    for(const inc of (cg?.[metric]?.incident_lifecycle||[]))out.push({type,status:inc?.status,start:inc?.start,resolved_at:inc?.resolved_at,end:inc?.resolved_at,max:inc?.max,max_ts:inc?.max_ts});
  }
  if(!out.length)return fallback||[];
  const start=Date.parse(a?.bounds?.start||''),end=Date.parse(a?.bounds?.end||'');
  for(const q of (fallback||[])){
    const ts=Date.parse((String(q?.status||'').toUpperCase()==='ATIVO'?q?.start:(q?.resolved_at||q?.end||q?.start))||'');
    const isCong=['P34','P135'].includes(String(q?.type||'').toUpperCase());
    const inSelected=Number.isFinite(ts)&&(!Number.isFinite(start)||ts>=start)&&(!Number.isFinite(end)||ts<=end);
    if(isCong&&inSelected)continue; // dentro do período, a mesma fonte do Destaques é autoritativa
    out.push(q);
  }
  return out;
}
function renderIncidents(r){
  const today=new Date().toLocaleDateString('en-CA',{timeZone:'America/Sao_Paulo'});
  const occurredToday=value=>{
    const stamp=Date.parse(value||'');
    return Number.isFinite(stamp)&&new Date(stamp).toLocaleDateString('en-CA',{timeZone:'America/Sao_Paulo'})===today;
  };
  const monitorById=new Map((window.toiSbcMonitorFeed?.incidentes||[]).filter(q=>q.id).map(q=>[q.id,q]));
  const sbc=[...monitorById.values()].map(q=>({
    id:q.id,
    type:q.type, status:q.status==='Ativo'?'ATIVO':q.status==='Evento'?'EVENTO':'RESOLVIDO',
    start:q.started_at, resolved_at:q.resolved_at||q.confirmed_at,
  }));
  const rows=[...(r||[]),...sbc].filter(q=>
    occurredToday(String(q?.status||'').toUpperCase()==='ATIVO'?q?.start:(q?.resolved_at||q?.end||q?.start))
  ).sort((a,b)=>{
    const ta=Date.parse((String(a?.status||'').toUpperCase()==='ATIVO'?a?.start:(a?.resolved_at||a?.end||a?.start))||'');
    const tb=Date.parse((String(b?.status||'').toUpperCase()==='ATIVO'?b?.start:(b?.resolved_at||b?.end||b?.start))||'');
    return (Number.isFinite(tb)?tb:0)-(Number.isFinite(ta)?ta:0);
  }).slice(0,8);
  $("recentIncidents").innerHTML=rows.map(q=>{
    const active=String(q?.status||'').toUpperCase()==='ATIVO';
    const informational=String(q?.status||'').toUpperCase()==='EVENTO';
    const eventTs=active?q?.start:(q?.resolved_at||q?.end||q?.start);
    const when=eventTs?eventStamp(eventTs):'—';
    const label=informational?`${q.type} confirmado`:active?`${q.type} elevado`:`${q.type} normalizado`;
    const detail=finiteNumber(q?.max)!==null?` · pico ${pct(q.max)}`:'';
    return `<div class="recent-row"><span>${when}</span><span class="chip ${active?'active':'resolved'}">${informational?'Evento':active?'ATIVO':'Resolvido'}</span><span>${esc(label)}${detail}</span></div>`;
  }).join("")||'<div class="recent-row"><span>—</span><span>Sem dados</span><span>Nenhum incidente registrado hoje</span></div>';
}
window.refreshSbcRecentIncidents=()=>{if(lastData)renderIncidents(reconcileRecentIncidents(lastAnalytics,lastData.recent_incidents||[]))};
function duration(a,b){if(!a||!b)return"—";let s=Math.max(0,Math.floor((new Date(b)-new Date(a))/1000)),h=Math.floor(s/3600);s%=3600;let m=Math.floor(s/60);return h?`${h}h ${m}m ${s%60}s`:`${m}m ${s%60}s`}
window.addEventListener('toi-authenticated',()=>init());
// Authentication may finish while this larger script is still downloading.
// The persistent flag also covers that ordering; initialization is idempotent.
if(window.toiAuthenticated)init();
window.addEventListener('online',()=>{if(window.toiAuthenticated){if(appInitialized){loadNow(true);schedule()}else init()}});
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&window.toiAuthenticated){if(appInitialized){loadNow();schedule()}else init()}});

// TOI R1.2.4 H2-C — Operational KPI Semantics + Event Attribution + Pressure Model Hardening
// TOI R1.2.4 H2-C1 — Operational Window + Gateway Story + Decision UX Hardening

// TOI R1.2.4 H2-C2 SEM — Executive P34 + Pressure + Capacity Semantics Hardening

// TOI R1.2.4 H2-C2 SEM H1 — Chart Zoom + Explainability + Capacity Comparatives

// TOI R1.2.4 H2-C2 SEM H3 UI CLOSURE — concise Highlights + chronological raw Gateway episodes + Story key reconciliation
// TOI R1.2.4 H3 INCIDENT HISTORY UI CLOSURE IH1 — reconciled incident lifecycle narrative
// TOI R1.2.6 H3 GATEWAY ICMP UI CLOSURE R1 — initial card reduced to Gateway | Ocupacao | ICMP
