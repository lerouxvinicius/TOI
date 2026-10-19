/* Consistent pointer and keyboard affordance for existing overview actions. */
(() => {
  const interactive='button,a,input,select,textarea,[data-radar-open],[data-radar-pending],.panel-link';
  function card(node,action,{existing=false,keyboard=true}={}){
    if(!node)return;
    node.classList.add('toi-clickable-card');
    node.setAttribute('role','button');node.tabIndex=0;
    if(!existing)node.addEventListener('click',event=>{
      if(event.target.closest(interactive))return;
      if(window.getSelection()?.toString())return;
      action();
    });
    if(keyboard)node.addEventListener('keydown',event=>{
      if(event.target!==node||!['Enter',' '].includes(event.key))return;
      event.preventDefault();action();
    });
  }
  function proxy(id){const target=document.getElementById(id);card(target?.closest('.panel'),()=>target.click());}
  for(const id of ['allGatewaysBtn','allTrunksBtn','openStuckAnalytics','openCapacityAnalytics','openIncidentHistory','radarExpand'])proxy(id);
  for(const [selector,name] of [['.agents-card','Agentes'],['.dialer-card','Discador'],['.recommendations','Recomendações']]){
    const node=document.querySelector(selector),open=()=>window.toiOpenSection?.(name);
    node?.querySelector('.panel-link')?.addEventListener('click',open);
    card(node,open);
  }
  const pressure=document.getElementById('operationalPressurePanel');
  card(pressure,()=>pressure.click(),{existing:true});
  card(document.getElementById('mlShadowCard'),()=>{}, {existing:true,keyboard:false});
})();
