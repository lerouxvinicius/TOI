(() => {
  'use strict';
  const nativeFetch = window.fetch.bind(window);
  window.toiCsrf = '';
  window.toiAuthenticated = false;
  window.fetch = (input, options = {}) => {
    let target = input;
    if (typeof input === 'string' && input.startsWith('/') && !input.startsWith('/toi/')) {
      target = '/toi' + input;
    }
    const method = String(options.method || 'GET').toUpperCase();
    if (method !== 'GET' && method !== 'HEAD' && typeof target === 'string' &&
        target.startsWith('/toi/api/') && !target.startsWith('/toi/api/admin/login')) {
      const headers = new Headers(options.headers || {});
      if (window.toiCsrf && !headers.has('X-CSRF-Token')) {
        headers.set('X-CSRF-Token', window.toiCsrf);
      }
      options = {...options, headers, credentials: 'same-origin'};
    }
    if(!options.signal)options={...options,signal:AbortSignal.timeout(15000)};
    return nativeFetch(target, options).then(response=>{
      if(response.status===401 && window.toiAuthenticated &&
         typeof target==='string' && target.startsWith('/toi/api/') &&
         !target.startsWith('/toi/api/admin/login')){
        window.dispatchEvent(new Event('toi-session-expired'));
      }
      return response;
    });
  };
})();
