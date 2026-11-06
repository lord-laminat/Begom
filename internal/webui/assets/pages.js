'use strict';
let catalogAfter='',galleryAfter='',catalogItems=[],catalogRequest=0,galleryRequest=0;
function pageTab(name){$$('.tab').forEach(n=>n.hidden=n.id!==name);$$('[data-tab]').forEach(n=>n.classList.remove('selected'));}
function selectionChanged(){
  $('#selection-count').textContent='Выбрано: '+selectedMedia.size+' / 10 000';
  $('#download-selection').disabled=!selectedMedia.size;
  $('#select-page').checked=catalogItems.length>0&&catalogItems.every(i=>selectedMedia.has(i.media_id));
}
function selectMaterial(id,checked){if(checked&&selectedMedia.size>=10000&&!selectedMedia.has(id))throw new Error('Не более 10 000 видео в одном задании');checked?selectedMedia.add(id):selectedMedia.delete(id);selectionChanged();}
async function loadCatalog(){
  const request=++catalogRequest,q=new URLSearchParams(new FormData($('#catalog-filter')));q.set('after',catalogAfter);q.set('limit','50');
  const v=await api('/catalog?'+q);if(request!==catalogRequest)return;catalogItems=v.items;
  const rows=v.items.map(i=>{
    const check=el('input');check.type='checkbox';check.dataset.mediaId=i.media_id;check.checked=selectedMedia.has(i.media_id);check.disabled=!operator();check.setAttribute('aria-label','Выбрать '+i.metadata.title);
    check.onchange=safe(()=>{try{selectMaterial(i.media_id,check.checked);}catch(e){check.checked=false;throw e;}});
    const title=el('div',i.metadata.title);title.append(el('small',i.metadata.canonical_id));
    return [check,i.external_id,title,(i.metadata.qualities||[]).map(q=>q.name+' · '+bytes(q.size_bytes)).join(', '),date(i.checked_at)];
  });
  $('#catalog-list').replaceChildren(rows.length?table(['Выбор','ID','Видео','Доступные качества','Проверено'],rows):el('p','Материалов пока нет. Запустите парсинг или измените фильтр.','empty-state'));
  $('#catalog-next').disabled=!v.next_after;$('#catalog-next').dataset.after=v.next_after;selectionChanged();
}
$('#catalog-filter').onsubmit=safe(async e=>{e.preventDefault();catalogAfter='';await loadCatalog();});
$('#catalog-refresh').onclick=safe(loadCatalog);
$('#catalog-next').onclick=safe(async()=>{catalogAfter=$('#catalog-next').dataset.after;await loadCatalog();});
$('#select-page').onchange=safe(async e=>{for(const i of catalogItems)selectMaterial(i.media_id,e.target.checked);await loadCatalog();});
$('#clear-selection').onclick=safe(async()=>{selectedMedia.clear();await loadCatalog();});
$('#download-selection').onclick=safe(()=>{if(!selectedMedia.size)throw new Error('Выберите видео');openJob(null,'download');});
function duration(ms){if(ms===null||ms===undefined)return '—';const seconds=Math.floor(ms/1000),m=Math.floor(seconds/60),s=seconds%60;return (m>=60?Math.floor(m/60)+':'+String(m%60).padStart(2,'0'):m)+':'+String(s).padStart(2,'0');}
function thumbnail(file){
  const cover=el('div',undefined,'video-cover'),placeholder=el('div','▶','video-placeholder');cover.append(placeholder);
  if(file.preview_state==='ready'){const image=el('img');image.alt='';image.loading='lazy';image.src='/api/files/'+encodeURIComponent(file.file_id)+'/thumbnail';image.onerror=()=>image.remove();cover.append(image);}
  cover.append(el('span',duration(file.duration_ms),'video-duration'));return cover;
}
async function loadGallery(){
  const request=++galleryRequest,q=new URLSearchParams(new FormData($('#gallery-filter')));q.set('after',galleryAfter);q.set('limit','24');
  const v=await api('/files?'+q);if(request!==galleryRequest)return;
  const grid=$('#gallery-list');grid.replaceChildren();
  for(const f of v.files){const card=el('article',undefined,'video-card'),link=el('a',undefined,'video-link');link.href='/watch/'+encodeURIComponent(f.file_id);link.append(thumbnail(f),el('h2',f.metadata.title||'Видео '+f.external_id));card.append(link,el('p',(f.metadata.quality||'Видео')+' · '+bytes(f.size_bytes),'muted'),el('small',date(f.downloaded_at)));grid.append(card);}
  if(!v.files.length)grid.append(el('p','Видео не найдены. Создайте скачивание из каталога или измените поиск.','empty-state'));
  $('#gallery-next').disabled=!v.next_after;$('#gallery-next').dataset.after=v.next_after;
}
$('#gallery-filter').onsubmit=safe(async e=>{e.preventDefault();galleryAfter='';const q=new URLSearchParams(new FormData(e.target));history.replaceState(null,'','/gallery?'+q);await loadGallery();});
$('#gallery-next').onclick=safe(async()=>{galleryAfter=$('#gallery-next').dataset.after;await loadGallery();});
async function loadWatch(){
  const id=location.pathname.slice('/watch/'.length),f=await api('/files/'+encodeURIComponent(id)),box=$('#watch-content');box.replaceChildren();
  const video=el('video');video.id='watch-player';video.controls=true;video.preload='metadata';video.src='/api/files/'+encodeURIComponent(id)+'/content';if(f.preview_state==='ready')video.poster='/api/files/'+encodeURIComponent(id)+'/thumbnail';
  const tools=el('div',undefined,'toolbar'),download=el('a','Скачать видео');download.id='watch-download';download.href=video.src+'?download=1';download.download='begom-'+f.external_id+({'video/mp4':'.mp4','video/webm':'.webm','video/quicktime':'.mov','video/x-matroska':'.mkv'}[f.mime_type]||'');
  tools.append(download);if(f.metadata.page_url){const source=el('a','Открыть источник');source.href=f.metadata.page_url;source.target='_blank';source.rel='noopener noreferrer';tools.append(source);}
  box.append(video,el('h1',f.metadata.title||'Видео '+f.external_id),el('p',(f.metadata.quality||'Видео')+' · '+duration(f.duration_ms)+' · '+bytes(f.size_bytes)+' · '+date(f.downloaded_at),'muted'),tools,el('p','ID источника: '+f.external_id+' · '+f.metadata.canonical_id,'muted'));
}
async function loadCurrentPage(){
  const p=location.pathname;
  if(p==='/catalog'){pageTab('catalog');await loadCatalog();}
  if(p==='/gallery'){pageTab('gallery');const q=new URLSearchParams(location.search);for(const [key,value]of q){const field=$('#gallery-filter').elements.namedItem(key);if(field)field.value=value;}await loadGallery();}
  if(p.startsWith('/watch/')){pageTab('watch');await loadWatch();}
}
document.addEventListener('begom:signedin',safe(loadCurrentPage));
if(user)safe(loadCurrentPage)();
// Refresh derived previews and newly published files while retaining filters and
// selection. Never reload the playback element or an open configuration form.
setInterval(()=>{if(!user||document.hidden||$('#job-dialog').open)return;if(location.pathname==='/gallery')safe(loadGallery)();if(location.pathname==='/catalog'&&!$('#catalog-filter').contains(document.activeElement))safe(loadCatalog)();},5000);
