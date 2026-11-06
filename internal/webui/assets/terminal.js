'use strict';
const terminal=new Terminal({cursorBlink:true,scrollback:5000,fontSize:15,theme:{background:'#101820',foreground:'#e8eef5'}}),fit=new FitAddon.FitAddon();
terminal.loadAddon(fit);terminal.open(document.querySelector('#terminal'));fit.fit();
let terminalConnection=null,terminalCSRF='',terminalAllowed=false;
const worker=location.pathname.split('/')[2],status=document.querySelector('#terminal-status'),connect=document.querySelector('#terminal-connect'),disconnect=document.querySelector('#terminal-disconnect');
document.querySelector('#terminal-title').textContent='SSH · '+worker;
async function terminalAPI(path,body){const headers={'API-Version':'2'};if(body!==undefined){headers['Content-Type']='application/json';headers['X-CSRF-Token']=terminalCSRF;headers['Idempotency-Key']=crypto.randomUUID().replaceAll('-','');}const r=await fetch('/api'+path,{method:body===undefined?'GET':'POST',headers,credentials:'same-origin',body:body===undefined?undefined:JSON.stringify(body)}),v=await r.json();if(!r.ok)throw new Error(v.message+' ('+v.code+')');return v;}
async function connectTerminal(){
  try{
    if(terminalConnection)terminalConnection.close();connect.disabled=true;status.textContent='Подключение…';
    const session=await terminalAPI('/workers/'+encodeURIComponent(worker)+'/ssh/sessions',{}),socket=new WebSocket('wss://'+location.host+'/api/workers/'+encodeURIComponent(worker)+'/ssh/stream');
    terminalConnection=socket;socket.binaryType='arraybuffer';socket.onopen=()=>{fit.fit();socket.send(JSON.stringify({type:'auth',ticket:session.ticket,cols:Math.max(20,Math.min(500,terminal.cols)),rows:Math.max(5,Math.min(200,terminal.rows))}));};
    socket.onmessage=e=>{if(socket!==terminalConnection)return;if(e.data instanceof ArrayBuffer){const data=new Uint8Array(e.data);terminal.write(data,()=>{if(socket.readyState===WebSocket.OPEN)socket.send(JSON.stringify({type:'ack',bytes:data.length}));});return;}const v=JSON.parse(e.data);if(v.type==='ready'){status.textContent='Подключено';disconnect.disabled=false;terminal.focus();}else if(v.type==='exit'){terminal.writeln('\r\n[Shell завершён, код '+v.code+']');}else if(v.type==='error'){terminal.writeln('\r\n'+v.message);}};
    socket.onclose=()=>{if(socket!==terminalConnection)return;status.textContent='Отключено';disconnect.disabled=true;connect.disabled=!terminalAllowed;terminal.writeln('\r\n[Соединение закрыто. Новое подключение создаст новую сессию.]');};
  }catch(e){status.textContent=e.message;connect.disabled=!terminalAllowed;}
}
terminal.onData(data=>{if(terminalConnection?.readyState!==WebSocket.OPEN)return;const bytes=new TextEncoder().encode(data);for(let i=0;i<bytes.length;i+=16384)terminalConnection.send(bytes.slice(i,i+16384));});
terminal.onResize(size=>{if(terminalConnection?.readyState===WebSocket.OPEN)terminalConnection.send(JSON.stringify({type:'resize',cols:Math.max(20,Math.min(500,size.cols)),rows:Math.max(5,Math.min(200,size.rows))}));});
new ResizeObserver(()=>fit.fit()).observe(document.querySelector('#terminal'));
connect.onclick=connectTerminal;disconnect.onclick=()=>terminalConnection?.close();window.addEventListener('beforeunload',()=>terminalConnection?.close());
(async()=>{try{const session=await terminalAPI('/auth/session');if(!session.user.roles.includes('administrator'))throw new Error('Терминал доступен администратору');terminalCSRF=session.csrf_token;terminalAllowed=true;await connectTerminal();}catch(e){status.textContent=e.message;const a=document.createElement('a');a.href='/';a.textContent='Открыть Begom и войти';document.querySelector('#terminal-notice').append(a);}})();
