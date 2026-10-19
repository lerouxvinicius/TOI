(() => {
  const screen = document.getElementById('loginScreen');
  const dashboard = document.getElementById('fitWrap');
  dashboard.inert = true;
  const form = document.getElementById('legacyLoginForm');
  const passwordForm = document.getElementById('legacyPasswordForm');
  const error = document.getElementById('legacyLoginError');
  let started = false;
  let expired = false;
  let passwordCsrf = '';
  let firstLogin = '';
  window.addEventListener('toi-session-expired', () => {
    window.toiAuthenticated = false;
    expired = true;
    started = false;
    window.toiCsrf = '';
    dashboard.inert = true;
    screen.hidden = false;
    form.hidden = false;
    passwordForm.hidden = true;
    error.textContent = 'Sua sessão expirou. Entre novamente para continuar.';
  });
  function enter(user, csrf='') {
    if(expired && !user.must_change){ location.reload(); return; }
    window.toiCsrf = csrf;
    if (user.must_change) {
      passwordCsrf = csrf;
      firstLogin = user.login;
      form.hidden = true;
      passwordForm.hidden = false;
      screen.hidden = false;
      dashboard.inert = true;
      error.textContent = 'Defina uma nova senha para acessar o TOI antigo.';
      return;
    }
    screen.hidden = true;
    dashboard.inert = false;
    window.toiAuthenticated = true;
    document.getElementById('signedUserName').textContent = user.name || user.login;
    document.getElementById('signedUserLogin').textContent = user.login || '—';
    document.getElementById('signedUserRole').textContent = user.role || '';
    if (!started) {
      started = true;
      window.dispatchEvent(new Event('toi-authenticated'));
    }
  }
  async function check() {
    try {
      const response = await fetch('/api/session', {credentials:'same-origin',cache:'no-store',signal:AbortSignal.timeout(10000)});
      if (response.ok) {
        const user=(await response.json()).user;
        const meResponse=await fetch('/api/admin/me',{credentials:'same-origin',cache:'no-store',signal:AbortSignal.timeout(10000)});
        if(!meResponse.ok)throw new Error('Sessão temporariamente indisponível');
        const me=await meResponse.json();
        enter(user,me.csrf);
      } else if(response.status!==401) {
        throw new Error('Sessão temporariamente indisponível');
      }
    } catch (_) { setTimeout(check,5000); }
  }
  form.addEventListener('submit', async event => {
    event.preventDefault();
    error.textContent = '';
    const submit = form.querySelector('button');
    submit.disabled = true;
    try {
      const fields = new FormData(form);
      const response = await fetch('/api/admin/login', {method:'POST',credentials:'same-origin',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({login:fields.get('login'),password:fields.get('password')})});
      const body = await response.json();
      if (!response.ok) throw new Error(body.error || 'Acesso negado');
      form.reset();
      enter(body.user,body.csrf);
    } catch (cause) { error.textContent = cause.message || 'Não foi possível entrar'; }
    finally { submit.disabled = false; }
  });
  passwordForm.addEventListener('submit',async event=>{
    event.preventDefault();
    const fields=new FormData(passwordForm);
    const fresh=String(fields.get('new_password')||'');
    if(fresh!==String(fields.get('confirm_password')||'')){
      error.textContent='A confirmação da nova senha não confere.';return;
    }
    const submit=passwordForm.querySelector('button');submit.disabled=true;error.textContent='';
    try{
      const response=await fetch('/api/admin/change-password',{method:'POST',credentials:'same-origin',
        headers:{'Content-Type':'application/json','X-CSRF-Token':passwordCsrf},
        body:JSON.stringify({current_password:fields.get('current_password'),new_password:fresh})});
      const body=await response.json();
      if(!response.ok)throw new Error(body.error||'Não foi possível trocar a senha');
      passwordForm.reset();passwordForm.hidden=true;form.hidden=false;
      const loginResponse=await fetch('/api/admin/login',{method:'POST',credentials:'same-origin',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({login:firstLogin,password:fresh})});
      const loginBody=await loginResponse.json();
      if(!loginResponse.ok)throw new Error('Senha alterada. Entre novamente com a nova senha.');
      firstLogin='';passwordCsrf='';
      enter(loginBody.user,loginBody.csrf);
    }catch(cause){error.textContent=cause.message||'Não foi possível trocar a senha';}
    finally{submit.disabled=false;}
  });
  const changeDialog=document.getElementById('changePasswordDialog');
  const changeForm=document.getElementById('changePasswordForm');
  const changeStatus=document.getElementById('changePasswordStatus');
  document.getElementById('changePasswordOpen').addEventListener('click',()=>{
    changeForm.reset();changeStatus.textContent='';changeDialog.showModal();
    changeForm.elements.current_password.focus();
  });
  document.getElementById('changePasswordCancel').addEventListener('click',()=>changeDialog.close());
  changeForm.addEventListener('submit',async event=>{
    event.preventDefault();
    const fields=new FormData(changeForm);
    const fresh=String(fields.get('new_password')||'');
    if(fresh!==String(fields.get('confirm_password')||'')){
      changeStatus.textContent='A confirmação da nova senha não confere.';return;
    }
    const submit=changeForm.querySelector('[type=submit]');submit.disabled=true;changeStatus.textContent='Salvando…';
    try{
      const me=await fetch('/api/admin/me',{credentials:'same-origin',cache:'no-store'}).then(r=>r.json());
      const response=await fetch('/api/admin/change-password',{method:'POST',credentials:'same-origin',
        headers:{'Content-Type':'application/json','X-CSRF-Token':me.csrf},
        body:JSON.stringify({current_password:fields.get('current_password'),new_password:fresh})});
      const body=await response.json();
      if(!response.ok)throw new Error(body.error||'Não foi possível alterar a senha.');
      changeForm.reset();changeStatus.textContent='Senha alterada. Entre novamente com a nova senha.';
      setTimeout(()=>location.reload(),1300);
    }catch(cause){changeStatus.textContent=cause.message||'Não foi possível alterar a senha.';}
    finally{submit.disabled=false;}
  });
  document.getElementById('legacyLogout').addEventListener('click', async () => {
    try {
      const me = await fetch('/api/admin/me', {credentials:'same-origin'}).then(r => r.json());
      await fetch('/api/admin/logout', {method:'POST',credentials:'same-origin',
        headers:{'Content-Type':'application/json','X-CSRF-Token':me.csrf},body:'{}'});
    } finally { location.reload(); }
  });
  const menu = document.getElementById('mobileMenuToggle');
  menu.addEventListener('click', () => {
    const expanded = document.body.classList.toggle('mobile-menu-open');
    menu.setAttribute('aria-expanded',String(expanded));
  });
  document.querySelectorAll('.sidebar .nav').forEach(item => item.addEventListener('click', () => {
    document.body.classList.remove('mobile-menu-open');
    menu.setAttribute('aria-expanded','false');
  }));
  check();
})();
