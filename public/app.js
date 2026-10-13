/* =========================================================
   Obsidian Neural Mind - Client-side Controller
   ========================================================= */

// State
const state = {
  activeTab: 'editor',
  currentNote: {
    title: '',
    content: '',
    path: '',
    links: [],
    tags: [],
    backlinks: []
  },
  notes: [],
  network: null,
  graphData: { nodes: [], edges: [] },
  isPhysicsActive: true,
  chatHistory: [],
  config: {
    vaultPath: 'D:\\estudio\\estudio',
    currentModel: 'qwen/qwen3.8-27b'
  }
};

// DOM Elements
const dom = {
  // Navigation
  tabs: document.querySelectorAll('.nav-tab'),
  tabContents: document.querySelectorAll('.tab-content'),
  vaultPathDisplay: document.getElementById('vaultPathDisplay'),
  vaultStatusDot: document.getElementById('vaultStatusDot'),
  modelSelect: document.getElementById('modelSelect'),
  notesCountBadge: document.getElementById('notesCountBadge'),
  graphCountBadge: document.getElementById('graphCountBadge'),

  // Editor
  noteTitleInput: document.getElementById('noteTitleInput'),
  noteContentInput: document.getElementById('noteContentInput'),
  previewContent: document.getElementById('previewContent'),
  btnAutoLink: document.getElementById('btnAutoLink'),
  btnEditorialTransform: document.getElementById('btnEditorialTransform'),
  btnExpandIdea: document.getElementById('btnExpandIdea'),
  btnSaveNote: document.getElementById('btnSaveNote'),
  btnNewNote: document.getElementById('btnNewNote'),
  wordCountStat: document.getElementById('wordCountStat'),
  charCountStat: document.getElementById('charCountStat'),
  linksCountStat: document.getElementById('linksCountStat'),
  saveStatusIndicator: document.getElementById('saveStatusIndicator'),
  aiInsightsPanel: document.getElementById('aiInsightsPanel'),
  outgoingLinksCloud: document.getElementById('outgoingLinksCloud'),
  backlinksCloud: document.getElementById('backlinksCloud'),
  dualPanes: document.getElementById('dualPanes'),
  editorPane: document.getElementById('editorPane'),
  previewPane: document.getElementById('previewPane'),
  emptyNoteBanner: document.getElementById('emptyNoteBanner'),
  emptyNoteBannerText: document.getElementById('emptyNoteBannerText'),
  btnFillWithAI: document.getElementById('btnFillWithAI'),

  // Graph
  neuralGraphNetwork: document.getElementById('neuralGraphNetwork'),
  graphSearchInput: document.getElementById('graphSearchInput'),
  btnGraphFit: document.getElementById('btnGraphFit'),
  btnGraphFreeze: document.getElementById('btnGraphFreeze'),
  btnGraphRefresh: document.getElementById('btnGraphRefresh'),
  btnDetectGaps: document.getElementById('btnDetectGaps'),
  gapsCountBadge: document.getElementById('gapsCountBadge'),
  gapsModal: document.getElementById('gapsModal'),
  closeGapsModal: document.getElementById('closeGapsModal'),
  gapsListContainer: document.getElementById('gapsListContainer'),
  gapsSelectedCount: document.getElementById('gapsSelectedCount'),
  btnSelectAllGaps: document.getElementById('btnSelectAllGaps'),
  btnBatchResolveGaps: document.getElementById('btnBatchResolveGaps'),
  nodeInfoCard: document.getElementById('nodeInfoCard'),
  nodeInfoTitle: document.getElementById('nodeInfoTitle'),
  nodeInfoExcerpt: document.getElementById('nodeInfoExcerpt'),
  nodeInfoLinksCount: document.getElementById('nodeInfoLinksCount'),
  btnOpenNodeInEditor: document.getElementById('btnOpenNodeInEditor'),
  btnAskAIAboutNode: document.getElementById('btnAskAIAboutNode'),
  closeNodeInfoBtn: document.getElementById('closeNodeInfoBtn'),

  // Chat
  chatMessages: document.getElementById('chatMessages'),
  chatInput: document.getElementById('chatInput'),
  btnSendChat: document.getElementById('btnSendChat'),

  // Vault
  vaultNotesGrid: document.getElementById('vaultNotesGrid'),
  vaultSearchInput: document.getElementById('vaultSearchInput'),
  btnVaultNewNote: document.getElementById('btnVaultNewNote'),

  // Libro Maestro
  btnCompileMasterBook: document.getElementById('btnCompileMasterBook'),
  btnPrintBookPdf: document.getElementById('btnPrintBookPdf'),
  btnOpenBookEditor: document.getElementById('btnOpenBookEditor'),
  btnRefreshBookPreview: document.getElementById('btnRefreshBookPreview'),
  bookStatsChapters: document.getElementById('bookStatsChapters'),
  bookStatsWords: document.getElementById('bookStatsWords'),
  bookStatsPages: document.getElementById('bookStatsPages'),
  bookStatsParts: document.getElementById('bookStatsParts'),
  bookPagesBadge: document.getElementById('bookPagesBadge'),
  bookLastUpdated: document.getElementById('bookLastUpdated'),
  bookReaderBody: document.getElementById('bookReaderBody'),
  bookFilePathDisplay: document.getElementById('bookFilePathDisplay'),

  // Settings Modal
  settingsBtn: document.getElementById('settingsBtn'),
  settingsModal: document.getElementById('settingsModal'),
  closeSettingsModal: document.getElementById('closeSettingsModal'),
  cancelSettingsBtn: document.getElementById('cancelSettingsBtn'),
  saveSettingsBtn: document.getElementById('saveSettingsBtn'),
  settingVaultPath: document.getElementById('settingVaultPath'),
  settingGroqKey: document.getElementById('settingGroqKey'),
  settingModelSelect: document.getElementById('settingModelSelect'),

  // Toast
  toastContainer: document.getElementById('toastContainer')
};

// =========================================================
// Initialization
// =========================================================
async function initApp() {
  setupEventListeners();
  await loadConfig();
  await loadNotes();
  initNetworkGraph();
  loadBookStatsAndPreview();
}

// Event Listeners
function setupEventListeners() {
  // Tabs
  dom.tabs.forEach(tab => {
    tab.addEventListener('click', () => switchTab(tab.dataset.tab));
  });

  // Editor Inputs
  dom.noteTitleInput.addEventListener('input', updateStats);
  dom.noteContentInput.addEventListener('input', () => {
    updateStats();
    renderMarkdownPreview();
  });

  // Toolbar Tools
  document.querySelectorAll('.tool-btn').forEach(btn => {
    btn.addEventListener('click', () => applyMarkdownTool(btn.dataset.tool));
  });

  // Segmented View Modes
  document.getElementById('viewModeSplit').addEventListener('click', (e) => setViewMode('split', e.target));
  document.getElementById('viewModeEdit').addEventListener('click', (e) => setViewMode('edit', e.target));
  document.getElementById('viewModePreview').addEventListener('click', (e) => setViewMode('preview', e.target));

  const btnPrintPdf = document.getElementById('btnPrintPdf');
  if (btnPrintPdf) {
    btnPrintPdf.addEventListener('click', () => {
      renderMarkdownPreview();
      window.print();
    });
  }

  // Libro Maestro Listeners
  if (dom.btnCompileMasterBook) {
    dom.btnCompileMasterBook.addEventListener('click', handleCompileMasterBook);
  }
  if (dom.btnPrintBookPdf) {
    dom.btnPrintBookPdf.addEventListener('click', handlePrintBookPdf);
  }
  const btnFloatingPdf = document.getElementById('btnExportPdfFloating');
  if (btnFloatingPdf) {
    btnFloatingPdf.addEventListener('click', handlePrintBookPdf);
  }
  if (dom.btnOpenBookEditor) {
    dom.btnOpenBookEditor.addEventListener('click', handleOpenBookInEditor);
  }
  if (dom.btnRefreshBookPreview) {
    dom.btnRefreshBookPreview.addEventListener('click', loadBookStatsAndPreview);
  }

  // Editor Actions
  dom.btnAutoLink.addEventListener('click', handleAutoLink);
  if (dom.btnEditorialTransform) {
    dom.btnEditorialTransform.addEventListener('click', handleEditorialTransform);
  }
  dom.btnExpandIdea.addEventListener('click', handleExpandIdea);
  dom.btnSaveNote.addEventListener('click', handleSaveNote);
  dom.btnNewNote.addEventListener('click', resetEditor);
  if (dom.btnFillWithAI) {
    dom.btnFillWithAI.addEventListener('click', handleFillCurrentNoteWithAI);
  }

  // Keyboard shortcut Ctrl+S
  window.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      handleSaveNote();
    }
  });

  // Graph controls
  dom.btnGraphFit.addEventListener('click', () => state.network && state.network.fit({ animation: true }));
  dom.btnGraphFreeze.addEventListener('click', toggleGraphPhysics);
  dom.btnGraphRefresh.addEventListener('click', refreshGraph);
  dom.graphSearchInput.addEventListener('input', handleGraphSearch);
  dom.closeNodeInfoBtn.addEventListener('click', () => dom.nodeInfoCard.style.display = 'none');

  // Gaps detector & modal controls
  if (dom.btnDetectGaps) {
    dom.btnDetectGaps.addEventListener('click', openGapsModal);
  }
  if (dom.closeGapsModal) {
    dom.closeGapsModal.addEventListener('click', () => dom.gapsModal.classList.remove('active'));
  }
  if (dom.btnSelectAllGaps) {
    dom.btnSelectAllGaps.addEventListener('click', () => {
      const cbs = document.querySelectorAll('.gap-checkbox');
      const allChecked = Array.from(cbs).every(c => c.checked);
      cbs.forEach(c => c.checked = !allChecked);
      updateGapsSelectedCount();
    });
  }
  if (dom.btnBatchResolveGaps) {
    dom.btnBatchResolveGaps.addEventListener('click', handleBatchResolveGaps);
  }

  // Chat
  dom.btnSendChat.addEventListener('click', handleSendChatMessage);
  dom.chatInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendChatMessage();
    }
  });

  document.querySelectorAll('.quick-prompt-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      dom.chatInput.value = btn.dataset.prompt;
      handleSendChatMessage();
    });
  });

  // Vault
  dom.vaultSearchInput.addEventListener('input', renderVaultGrid);
  dom.btnVaultNewNote.addEventListener('click', () => {
    resetEditor();
    switchTab('editor');
  });

  // Settings
  dom.settingsBtn.addEventListener('click', openSettings);
  dom.vaultPathDisplay.parentElement.addEventListener('click', openSettings);
  dom.closeSettingsModal.addEventListener('click', closeSettings);
  dom.cancelSettingsBtn.addEventListener('click', closeSettings);
  dom.saveSettingsBtn.addEventListener('click', handleSaveSettings);
  dom.modelSelect.addEventListener('change', handleQuickModelChange);
}

// =========================================================
// Tab Management
// =========================================================
function switchTab(tabId) {
  state.activeTab = tabId;
  dom.tabs.forEach(t => t.classList.toggle('active', t.dataset.tab === tabId));
  dom.tabContents.forEach(c => c.classList.toggle('active', c.id === `tab-${tabId}`));

  if (tabId === 'graph') {
    setTimeout(() => {
      if (state.network) {
        state.network.fit();
      } else {
        initNetworkGraph();
      }
    }, 150);
  } else if (tabId === 'vault') {
    renderVaultGrid();
  } else if (tabId === 'book') {
    loadBookStatsAndPreview();
  }
}

function setViewMode(mode, targetBtn) {
  document.querySelectorAll('.seg-btn').forEach(b => b.classList.remove('active'));
  targetBtn.classList.add('active');

  if (mode === 'split') {
    dom.editorPane.style.display = 'block';
    dom.previewPane.style.display = 'block';
  } else if (mode === 'edit') {
    dom.editorPane.style.display = 'block';
    dom.previewPane.style.display = 'none';
  } else if (mode === 'preview') {
    dom.editorPane.style.display = 'none';
    dom.previewPane.style.display = 'block';
    renderMarkdownPreview();
  }
}

// =========================================================
// API & Data Sync
// =========================================================
async function loadConfig() {
  try {
    const res = await fetch('/api/config');
    const data = await res.json();
    state.config = data;
    dom.vaultPathDisplay.textContent = data.vaultPath;
    dom.settingVaultPath.value = data.vaultPath;
    dom.modelSelect.value = data.currentModel;
    dom.settingModelSelect.value = data.currentModel;

    if (data.vaultExists) {
      dom.vaultStatusDot.classList.add('online');
      dom.vaultStatusDot.title = 'Bóveda conectada en ' + data.vaultPath;
    } else {
      dom.vaultStatusDot.classList.remove('online');
      dom.vaultStatusDot.title = 'Ruta no encontrada';
    }
  } catch (err) {
    showToast('Error cargando configuración', 'error');
  }
}

async function loadNotes() {
  try {
    const res = await fetch('/api/notes');
    const data = await res.json();
    state.notes = data.notes || [];
    dom.notesCountBadge.textContent = state.notes.length;
    renderVaultGrid();
    await loadVaultGaps();
  } catch (err) {
    console.error('Error cargando notas:', err);
  }
}

async function loadVaultGaps() {
  try {
    const res = await fetch('/api/vault/gaps');
    const data = await res.json();
    state.vaultGaps = data;
    if (dom.gapsCountBadge) {
      dom.gapsCountBadge.textContent = data.totalGaps || 0;
    }
    return data;
  } catch (err) {
    console.error('Error cargando lagunas:', err);
  }
}

window.openGapsModal = async function(customSuggestedTitles) {
  const data = await loadVaultGaps();
  if (!data) return;

  const existingSet = new Set(state.notes.map(n => n.name.toLowerCase()));

  // Extract conceptual gaps from chat or argument
  let conceptualGaps = [];
  const candidateList = customSuggestedTitles || state.lastChatGaps || [];
  candidateList.forEach(title => {
    const clean = title.trim();
    if (clean && !existingSet.has(clean.toLowerCase()) && !conceptualGaps.includes(clean)) {
      conceptualGaps.push(clean);
    }
  });

  let html = '';

  // 1. Conceptual AI Gaps
  if (conceptualGaps.length > 0) {
    html += `
      <div style="margin-bottom: 16px;">
        <div style="font-size: 11px; font-weight: 700; color: #a78bfa; text-transform: uppercase; margin-bottom: 8px;">
          🧠 Lagunas Conceptuales detectadas por el Cerebro IA (${conceptualGaps.length}):
        </div>
        ${conceptualGaps.map(title => `
          <div style="display: flex; align-items: center; justify-content: space-between; background: var(--bg-surface-elevated); padding: 8px 12px; border-radius: var(--radius-md); margin-bottom: 6px; border: 1px solid rgba(139, 92, 246, 0.4);">
            <label style="display: flex; align-items: center; gap: 8px; cursor: pointer; font-size: 13px; font-weight: 600; flex: 1;">
              <input type="checkbox" class="gap-checkbox" value="${title}" checked />
              <span>💡 [[${title}]]</span>
              <span style="font-size: 11px; color: #a78bfa; font-weight: normal;">(Recomendación del Cerebro IA)</span>
            </label>
            <button class="btn btn-secondary btn-sm" onclick="quickCreateStubNote('${title}', this)">✨ Redactar</button>
          </div>
        `).join('')}
      </div>
    `;
  }

  // 2. Empty notes
  if (data.emptyNotes && data.emptyNotes.length > 0) {
    html += `
      <div style="margin-bottom: 16px;">
        <div style="font-size: 11px; font-weight: 700; color: #f59e0b; text-transform: uppercase; margin-bottom: 8px;">
          ⚠️ Notas existentes pero vacías (${data.emptyNotes.length}):
        </div>
        ${data.emptyNotes.map(n => `
          <div style="display: flex; align-items: center; justify-content: space-between; background: var(--bg-surface-elevated); padding: 8px 12px; border-radius: var(--radius-md); margin-bottom: 6px; border: 1px solid rgba(245, 158, 11, 0.3);">
            <label style="display: flex; align-items: center; gap: 8px; cursor: pointer; font-size: 13px; font-weight: 600;">
              <input type="checkbox" class="gap-checkbox" value="${n.name}" checked />
              <span>📄 [[${n.name}]]</span>
              <span style="font-size: 11px; color: #f59e0b; font-weight: normal;">(Vacía - 0 bytes)</span>
            </label>
            <button class="btn btn-secondary btn-sm" onclick="quickCreateStubNote('${n.name}', this)">✨ Redactar</button>
          </div>
        `).join('')}
      </div>
    `;
  }

  // 3. Ghost nodes (uncreated links from disk)
  if (data.missingNotes && data.missingNotes.length > 0) {
    html += `
      <div>
        <div style="font-size: 11px; font-weight: 700; color: #06b6d4; text-transform: uppercase; margin-bottom: 8px;">
          🔗 Nodos Fantasma (Mencionadas pero pendientes de crear - ${data.missingNotes.length}):
        </div>
        ${data.missingNotes.map(m => `
          <div style="display: flex; align-items: center; justify-content: space-between; background: var(--bg-surface-elevated); padding: 8px 12px; border-radius: var(--radius-md); margin-bottom: 6px; border: 1px solid var(--border-subtle);">
            <label style="display: flex; align-items: center; gap: 8px; cursor: pointer; font-size: 13px; font-weight: 600; flex: 1;">
              <input type="checkbox" class="gap-checkbox" value="${m.title}" checked />
              <span>🔗 [[${m.title}]]</span>
              <span style="font-size: 11px; color: var(--text-muted); font-weight: normal;">Mencionada en: ${m.referencedBy.slice(0, 2).map(r => `[[${r}]]`).join(', ')}</span>
            </label>
            <button class="btn btn-secondary btn-sm" onclick="quickCreateStubNote('${m.title}', this)">✨ Redactar</button>
          </div>
        `).join('')}
      </div>
    `;
  }

  const grandTotal = conceptualGaps.length + (data.emptyNotes ? data.emptyNotes.length : 0) + (data.missingNotes ? data.missingNotes.length : 0);

  if (grandTotal === 0) {
    html = `
      <div style="text-align: center; padding: 40px 20px; color: var(--accent-emerald);">
        <div style="font-size: 32px; margin-bottom: 8px;">🎉</div>
        <h4>¡Tu red neuronal no tiene lagunas pendientes!</h4>
        <p style="font-size: 13px; color: var(--text-muted);">Todas las notas enlazadas y sugeridas existen y tienen contenido.</p>
      </div>
    `;
  }

  dom.gapsListContainer.innerHTML = html;
  updateGapsSelectedCount();

  document.querySelectorAll('.gap-checkbox').forEach(cb => {
    cb.addEventListener('change', updateGapsSelectedCount);
  });

  dom.gapsModal.classList.add('active');
};

function updateGapsSelectedCount() {
  const checked = document.querySelectorAll('.gap-checkbox:checked');
  dom.gapsSelectedCount.textContent = `${checked.length} notas seleccionadas`;
}

async function handleBatchResolveGaps() {
  const checked = Array.from(document.querySelectorAll('.gap-checkbox:checked')).map(cb => cb.value);
  if (checked.length === 0) {
    showToast('Selecciona al menos una nota para redactar.', 'info');
    return;
  }

  setBtnLoading(dom.btnBatchResolveGaps, true, `Redactando ${checked.length} notas...`);
  showToast(`🧠 La IA está redactando y conectando ${checked.length} notas en tu Obsidian...`, 'info');

  try {
    const res = await fetch('/api/ai/batch-fill-gaps', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ titles: checked })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    showToast(`¡${data.generatedCount} notas redactadas y conectadas con éxito!`, 'success');
    dom.gapsModal.classList.remove('active');
    await loadNotes();
    await loadVaultGaps();
  } catch (err) {
    showToast('Error redactando notas: ' + err.message, 'error');
  } finally {
    setBtnLoading(dom.btnBatchResolveGaps, false, '⚡ Redactar Seleccionadas con IA');
  }
}

// =========================================================
// Markdown Tools & Live Preview
// =========================================================
function applyMarkdownTool(tool) {
  const textarea = dom.noteContentInput;
  const start = textarea.selectionStart;
  const end = textarea.selectionEnd;
  const sel = textarea.value.substring(start, end);

  let replacement = '';
  switch (tool) {
    case 'bold':
      replacement = `**${sel || 'texto en negrita'}**`;
      break;
    case 'italic':
      replacement = `*${sel || 'texto en cursiva'}*`;
      break;
    case 'h1':
      replacement = `\n# ${sel || 'Título Principal'}\n`;
      break;
    case 'h2':
      replacement = `\n## ${sel || 'Sección'}\n`;
      break;
    case 'list':
      replacement = `\n- ${sel || 'Elemento de lista'}`;
      break;
    case 'wikilink':
      replacement = `[[${sel || 'Nota Vinculada'}]]`;
      break;
    case 'tag':
      replacement = `#${sel || 'tema'}`;
      break;
  }

  textarea.setRangeText(replacement, start, end, 'end');
  textarea.focus();
  updateStats();
  renderMarkdownPreview();
}

function renderMarkdownPreview() {
  let content = dom.noteContentInput.value;
  if (!content.trim()) {
    dom.previewContent.innerHTML = `
      <div class="empty-preview-tip">
        <div class="tip-icon">💡</div>
        <h3>Vista Previa en Vivo</h3>
        <p>A medida que escribas o la IA inserte enlaces <code>[[...]]</code>, podrás ver cómo se renderizará en tu Obsidian.</p>
      </div>`;
    return;
  }

  // Convert [[Wikilink|Alias]] and [[Wikilink]] to clickable Obsidian pills
  content = content.replace(/\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|([^\]]+))?\]\]/g, (match, link, alias) => {
    const label = alias || link;
    return `<a class="obsidian-link" data-link="${link.trim()}">🔗 [[${label.trim()}]]</a>`;
  });

  // Marked parser
  if (window.marked) {
    dom.previewContent.innerHTML = window.marked.parse(content);
    // Clickable links in preview
    dom.previewContent.querySelectorAll('.obsidian-link').forEach(linkEl => {
      linkEl.addEventListener('click', () => {
        openNoteByName(linkEl.dataset.link);
      });
    });
  }
}

function updateStats() {
  const title = dom.noteTitleInput.value.trim();
  const text = dom.noteContentInput.value;

  const words = text.trim() ? text.trim().split(/\s+/).length : 0;
  const chars = text.length;

  // Extract [[links]]
  const links = [];
  const rx = /\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|[^\]]+)?\]\]/g;
  let m;
  while ((m = rx.exec(text)) !== null) {
    if (!links.includes(m[1].trim())) links.push(m[1].trim());
  }

  dom.wordCountStat.textContent = `${words} palabras`;
  dom.charCountStat.textContent = `${chars} caracteres`;
  dom.linksCountStat.textContent = `${links.length} enlaces [[...]]`;

  // Toggle Empty Note AI assistant banner
  if (title && text.trim().length === 0) {
    dom.emptyNoteBanner.style.display = 'flex';
    dom.emptyNoteBannerText.textContent = `La nota "[[${title}]]" está vacía. ¿Quieres que la IA la redacte con definiciones y enlaces basados en tu estudio?`;
  } else {
    dom.emptyNoteBanner.style.display = 'none';
  }

  renderLinksClouds(links);
}

// Handler for the empty note banner "Redactar con IA"
async function handleFillCurrentNoteWithAI() {
  const title = getEffectiveTitle();
  if (!title) {
    showToast('Ingresa un título para la nota primero.', 'info');
    dom.noteTitleInput.focus();
    return;
  }

  setBtnLoading(dom.btnFillWithAI, true, 'Redactando con IA...');
  showToast(`🧠 La IA está redactando la nota [[${title}]] usando el contexto de tu bóveda...`, 'info');

  try {
    const res = await fetch('/api/ai/generate-linked-note', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title,
        sourceNoteTitle: '',
        sourceNoteContent: ''
      })
    });

    const data = await res.json();
    if (data.error) throw new Error(data.error);

    dom.noteContentInput.value = data.content;
    renderMarkdownPreview();
    updateStats();
    showToast(`¡Nota [[${title}]] redactada y guardada en Obsidian!`, 'success');
    await loadNotes();
  } catch (err) {
    showToast('Error generando nota: ' + err.message, 'error');
  } finally {
    setBtnLoading(dom.btnFillWithAI, false, 'Redactar con IA');
  }
}

function renderLinksClouds(links) {
  if (links.length === 0) {
    dom.outgoingLinksCloud.innerHTML = '<span class="empty-tag-msg">Sin enlaces en esta nota</span>';
  } else {
    dom.outgoingLinksCloud.innerHTML = links.map(l => `
      <span class="link-pill" onclick="openNoteByName('${l}')">[[${l}]]</span>
    `).join('');
  }

  // Backlinks
  const currentTitle = dom.noteTitleInput.value.trim().toLowerCase();
  const foundNote = state.notes.find(n => n.name.toLowerCase() === currentTitle);
  if (foundNote && foundNote.backlinks && foundNote.backlinks.length > 0) {
    dom.backlinksCloud.innerHTML = foundNote.backlinks.map(b => `
      <span class="link-pill" onclick="openNoteByName('${b}')">[[${b}]]</span>
    `).join('');
  } else {
    dom.backlinksCloud.innerHTML = '<span class="empty-tag-msg">Sin notas que la referencien</span>';
  }
}

// =========================================================
// AI Auto-Linking ("⚡ Conectar con IA")
// =========================================================
async function handleAutoLink() {
  const title = getEffectiveTitle();
  const content = dom.noteContentInput.value.trim();

  if (!content) {
    showToast('Escribe algo en tu nota primero para que la IA pueda analizarlo.', 'info');
    return;
  }

  setBtnLoading(dom.btnAutoLink, true, 'Conectando con red...');

  try {
    const res = await fetch('/api/ai/autolink', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title, content })
    });

    const data = await res.json();
    if (data.error) throw new Error(data.error);

    // Apply enhanced text with [[links]]
    if (data.enhancedContent) {
      dom.noteContentInput.value = data.enhancedContent;
      renderMarkdownPreview();
      updateStats();
    }

    // Render Insights Panel
    renderAiInsights(data);
    showToast(`¡Conectado! Se detectaron enlaces y conceptos en tu red.`, 'success');
  } catch (err) {
    showToast('Error al conectar con IA: ' + err.message, 'error');
  } finally {
    setBtnLoading(dom.btnAutoLink, false, 'Conectar con IA');
  }
}

function renderAiInsights(data) {
  let html = '';
  html += `
    <div style="display: flex; align-items: center; gap: 6px; color: #10b981; font-size: 12px; font-weight: 600; margin-bottom: 12px; background: rgba(16, 185, 129, 0.1); padding: 8px 12px; border-radius: var(--radius-sm); border-left: 3px solid #10b981;">
      <span>✨</span>
      <span>Ortografía y acentos pulidos sin alterar tu voz</span>
    </div>
  `;

  if (data.neuralInsight) {
    html += `
      <div class="insight-box">
        <strong>🧠 Conexión en tu red:</strong><br/>
        ${data.neuralInsight}
      </div>
    `;
  }

  if (data.newLinksCreated && data.newLinksCreated.length > 0) {
    html += `
      <div class="section-label">Enlaces integrados:</div>
      <div class="tags-cloud mb-3" style="margin-bottom: 12px;">
        ${data.newLinksCreated.map(l => `<span class="link-pill" onclick="openNoteByName('${l}')">[[${l}]]</span>`).join('')}
      </div>
    `;
  }

  if (data.suggestedNewNotes && data.suggestedNewNotes.length > 0) {
    html += `
      <div class="section-label">Nuevas notas recomendadas para tu red:</div>
      <div class="suggested-list" style="margin-bottom: 12px;">
        ${data.suggestedNewNotes.map(n => `
          <div class="suggested-item">
            <span>📄 ${n}</span>
            <button class="suggested-item-btn" onclick="quickCreateStubNote('${n}', this)">✨ Redactar con IA</button>
          </div>
        `).join('')}
      </div>
    `;
  }

  if (data.suggestedTags && data.suggestedTags.length > 0) {
    html += `
      <div class="section-label">Etiquetas sugeridas:</div>
      <div class="tags-cloud">
        ${data.suggestedTags.map(t => `<span class="tag-pill" onclick="insertTag('${t}')">${t}</span>`).join('')}
      </div>
    `;
  }

  dom.aiInsightsPanel.innerHTML = html;
}

// Quick insert suggested tag
window.insertTag = function(tag) {
  const text = dom.noteContentInput.value;
  if (!text.includes(tag)) {
    dom.noteContentInput.value = text.trim() + `\n\n${tag}`;
    renderMarkdownPreview();
    showToast(`Etiqueta ${tag} agregada`, 'info');
  }
};

// =========================================================
// Commercial E-book / Editorial Manual Converter
// =========================================================
async function handleEditorialTransform() {
  const title = getEffectiveTitle();
  const content = dom.noteContentInput.value.trim();

  if (!content) {
    showToast('Escribe o abre una nota técnica primero para transformarla en manual editorial.', 'info');
    return;
  }

  setBtnLoading(dom.btnEditorialTransform, true, 'Redactando E-book...');
  showToast('📖 Transformando apunte en Capítulo de Libro / Manual Vendible con IA...', 'info');

  try {
    const res = await fetch('/api/ai/editorial-transform', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title,
        content,
        author: 'Luis'
      })
    });

    const data = await res.json();
    if (data.error) throw new Error(data.error);

    dom.noteContentInput.value = data.content;
    renderMarkdownPreview();
    updateStats();
    showToast('¡Capítulo de libro editorial generado! Dale a "Exportar PDF" para imprimirlo o venderlo.', 'success');
  } catch (err) {
    showToast('Error generando formato editorial: ' + err.message, 'error');
  } finally {
    setBtnLoading(dom.btnEditorialTransform, false, 'Formato Libro / E-book');
  }
}

// =========================================================
// AI Expand Idea ("✨ Expandir Idea")
// =========================================================
async function handleExpandIdea() {
  const title = dom.noteTitleInput.value.trim() || 'Concepto Nuevo';
  const idea = dom.noteContentInput.value.trim();

  setBtnLoading(dom.btnExpandIdea, true, 'Expandiendo con IA...');

  try {
    const res = await fetch('/api/ai/expand', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title, idea })
    });

    const data = await res.json();
    if (data.error) throw new Error(data.error);

    dom.noteContentInput.value = data.content;
    renderMarkdownPreview();
    updateStats();
    showToast('¡Idea expandida con estructura y enlaces de Obsidian!', 'success');
  } catch (err) {
    showToast('Error expandiendo idea: ' + err.message, 'error');
  } finally {
    setBtnLoading(dom.btnExpandIdea, false, 'Expandir Idea');
  }
}

// Auto-extract and clean title from input or note content
function getEffectiveTitle() {
  let rawTitle = dom.noteTitleInput.value.trim();

  // If title field is empty, detect from content (e.g. # Spark or first line)
  if (!rawTitle) {
    const text = dom.noteContentInput.value.trim();
    if (text) {
      const headingMatch = text.match(/^#+\s*(.+)$/m);
      if (headingMatch && headingMatch[1].trim()) {
        rawTitle = headingMatch[1].trim();
      } else {
        const firstLine = text.split('\n').find(l => l.trim().length > 0);
        if (firstLine) {
          rawTitle = firstLine.replace(/^[#*\s-]+/, '').trim();
        }
      }
    }
  }

  // Sanitize: strip any leading '#' or extra symbols
  const cleanTitle = rawTitle.replace(/^#+\s*/, '').replace(/[\\/:*?"<>|]/g, '-').trim();
  if (cleanTitle && dom.noteTitleInput.value !== cleanTitle) {
    dom.noteTitleInput.value = cleanTitle;
  }
  return cleanTitle;
}

// =========================================================
// Save Note Directly to Obsidian Vault ("💾 Guardar")
// =========================================================
async function handleSaveNote() {
  const title = getEffectiveTitle();
  const content = dom.noteContentInput.value;

  if (!title) {
    showToast('Ingresa un título para tu nota o escribe un encabezado con # en el texto.', 'info');
    dom.noteTitleInput.focus();
    return;
  }

  dom.saveStatusIndicator.textContent = 'Guardando en Obsidian...';

  try {
    const res = await fetch('/api/notes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title, content })
    });

    const data = await res.json();
    if (data.error) throw new Error(data.error);

    dom.saveStatusIndicator.textContent = '✓ Guardado en tu bóveda de Obsidian';
    showToast(`Nota "${title}.md" guardada en ${state.config.vaultPath}`, 'success');

    // Reload notes and refresh graph
    await loadNotes();
  } catch (err) {
    dom.saveStatusIndicator.textContent = 'Error al guardar';
    showToast('Error al guardar: ' + err.message, 'error');
  }
}

function resetEditor() {
  dom.noteTitleInput.value = '';
  dom.noteContentInput.value = '';
  dom.aiInsightsPanel.innerHTML = `
    <div class="empty-state">
      <p>Escribe tu nota y haz clic en <strong>⚡ Conectar con IA</strong> para descubrir enlaces cruzados, conceptos clave y nuevas notas recomendadas.</p>
    </div>`;
  renderMarkdownPreview();
  updateStats();
  dom.saveStatusIndicator.textContent = 'Nueva nota en blanco';
  dom.noteTitleInput.focus();
}

window.quickCreateStubNote = async function(title, btnElement) {
  const currentTitle = getEffectiveTitle();
  const currentContent = dom.noteContentInput.value;

  if (btnElement) {
    btnElement.disabled = true;
    btnElement.textContent = '⏳ Redactando...';
  }

  showToast(`🧠 La IA está redactando la nota [[${title}]] con todo el contexto...`, 'info');

  try {
    const res = await fetch('/api/ai/generate-linked-note', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title,
        sourceNoteTitle: currentTitle,
        sourceNoteContent: currentContent
      })
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    if (btnElement) {
      btnElement.textContent = '✓ Redactada';
      btnElement.style.background = '#10b981';
      btnElement.style.borderColor = '#10b981';
    }

    showToast(`¡Nota [[${title}]] redactada y guardada en Obsidian!`, 'success');
    await loadNotes();

    // Ask if user wants to view it now
    setTimeout(() => {
      if (confirm(`¡La IA acaba de redactar la nota [[${title}]] con todo su contenido y enlaces!\n\n¿Deseas abrirla en el editor ahora mismo para revisarla?`)) {
        openNoteByName(title);
      }
    }, 400);
  } catch (err) {
    if (btnElement) {
      btnElement.disabled = false;
      btnElement.textContent = 'Reintentar';
    }
    showToast('Error generando nota: ' + err.message, 'error');
  }
};

window.openNoteByName = async function(noteName) {
  try {
    const res = await fetch(`/api/notes/${encodeURIComponent(noteName)}`);
    if (res.status === 404) {
      // Prompt to create it with AI
      const wantsAi = confirm(`La nota "[[${noteName}]]" aún no existe en tu bóveda.\n\n¿Quieres que la IA la redacte automáticamente con todo el contexto y definiciones sobre este tema?`);
      if (wantsAi) {
        showToast(`🧠 Redactando nota [[${noteName}]] con IA...`, 'info');
        const resGen = await fetch('/api/ai/generate-linked-note', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            title: noteName,
            sourceNoteTitle: getEffectiveTitle(),
            sourceNoteContent: dom.noteContentInput.value
          })
        });
        const genData = await resGen.json();
        if (genData.content) {
          dom.noteTitleInput.value = noteName;
          dom.noteContentInput.value = genData.content;
          switchTab('editor');
          renderMarkdownPreview();
          updateStats();
          await loadNotes();
          showToast(`¡Nota [[${noteName}]] generada con éxito!`, 'success');
          return;
        }
      }
      return;
    }

    const data = await res.json();
    dom.noteTitleInput.value = data.name;
    dom.noteContentInput.value = data.content;
    switchTab('editor');
    renderMarkdownPreview();
    updateStats();
    showToast(`Abierta [[${data.name}]]`, 'info');
  } catch (err) {
    showToast('Error abriendo nota: ' + err.message, 'error');
  }
};

// =========================================================
// Neural Graph Network (vis-network)
// =========================================================
function initNetworkGraph() {
  if (!dom.neuralGraphNetwork) return;

  const container = dom.neuralGraphNetwork;
  const data = {
    nodes: new vis.DataSet([]),
    edges: new vis.DataSet([])
  };

  const options = {
    nodes: {
      shape: 'dot',
      font: {
        color: '#f8fafc',
        size: 13,
        face: 'Plus Jakarta Sans, sans-serif',
        strokeWidth: 2,
        strokeColor: '#090b10'
      },
      borderWidth: 2,
      shadow: {
        enabled: true,
        color: 'rgba(139, 92, 246, 0.4)',
        size: 10
      }
    },
    edges: {
      width: 1.5,
      color: {
        color: 'rgba(139, 92, 246, 0.35)',
        highlight: '#ec4899',
        hover: '#38bdf8'
      },
      smooth: {
        type: 'continuous'
      }
    },
    physics: {
      solver: 'forceAtlas2Based',
      forceAtlas2Based: {
        gravitationalConstant: -38,
        centralGravity: 0.015,
        springLength: 90,
        springConstant: 0.08,
        damping: 0.4
      },
      maxVelocity: 50,
      minVelocity: 0.1,
      stabilization: { iterations: 150 }
    },
    interaction: {
      hover: true,
      tooltipDelay: 100,
      zoomView: true
    }
  };

  state.network = new vis.Network(container, data, options);

  // Click on node
  state.network.on('click', (params) => {
    if (params.nodes.length > 0) {
      const nodeId = params.nodes[0];
      const clickedNode = state.graphData.nodes.find(n => n.id === nodeId);
      if (clickedNode) {
        showNodeInfoCard(clickedNode);
      }
    } else {
      dom.nodeInfoCard.style.display = 'none';
    }
  });

  refreshGraph();
}

async function refreshGraph() {
  try {
    const res = await fetch('/api/graph');
    const data = await res.json();
    state.graphData = data;

    dom.graphCountBadge.textContent = data.nodes.length;

    if (state.network) {
      const nodesDataSet = new vis.DataSet(data.nodes.map(n => ({
        id: n.id,
        label: n.label,
        size: n.size,
        color: {
          background: n.color,
          border: n.exists ? '#a78bfa' : '#38bdf8',
          highlight: { background: '#ec4899', border: '#fff' }
        },
        title: `${n.label} (${n.exists ? 'Nota' : 'Concepto'})`
      })));

      const edgesDataSet = new vis.DataSet(data.edges);
      state.network.setData({ nodes: nodesDataSet, edges: edgesDataSet });
    }
  } catch (err) {
    console.error('Error cargando grafo:', err);
  }
}

function updateGraphData() {
  refreshGraph();
}

function showNodeInfoCard(node) {
  dom.nodeInfoTitle.textContent = `[[${node.label}]]`;
  dom.nodeInfoExcerpt.textContent = node.excerpt || 'Nota conceptual';
  dom.nodeInfoLinksCount.textContent = node.exists ? 'Nota guardada en Obsidian' : 'Concepto pendiente de crear';

  dom.btnOpenNodeInEditor.onclick = () => {
    openNoteByName(node.id);
    dom.nodeInfoCard.style.display = 'none';
  };

  dom.btnAskAIAboutNode.onclick = () => {
    dom.chatInput.value = `Explícame en detalle cómo se relaciona [[${node.id}]] con el resto de mis notas y qué ideas clave debería desarrollar sobre este concepto.`;
    switchTab('chat');
    handleSendChatMessage();
    dom.nodeInfoCard.style.display = 'none';
  };

  dom.nodeInfoCard.style.display = 'block';
}

function toggleGraphPhysics() {
  state.isPhysicsActive = !state.isPhysicsActive;
  state.network.setOptions({ physics: { enabled: state.isPhysicsActive } });
  dom.btnGraphFreeze.textContent = state.isPhysicsActive ? '❄️ Congelar Física' : '▶️ Reactivar Física';
  showToast(state.isPhysicsActive ? 'Física activada' : 'Grafo congelado', 'info');
}

function handleGraphSearch() {
  const query = dom.graphSearchInput.value.toLowerCase().trim();
  if (!query || !state.network) return;

  const found = state.graphData.nodes.find(n => n.label.toLowerCase().includes(query));
  if (found) {
    state.network.focus(found.id, {
      scale: 1.4,
      animation: { duration: 600, easingFunction: 'easeInOutQuad' }
    });
    state.network.selectNodes([found.id]);
    showNodeInfoCard(found);
  }
}

// =========================================================
// Cerebro IA (Chat RAG)
// =========================================================
async function handleSendChatMessage() {
  const text = dom.chatInput.value.trim();
  if (!text) return;

  // Add user message to UI
  appendChatMessage('user', text);
  dom.chatInput.value = '';

  state.chatHistory.push({ role: 'user', content: text });

  // Current note context if any
  const currentNoteName = dom.noteTitleInput.value.trim();
  const currentNoteContent = dom.noteContentInput.value;

  // Show typing indicator
  const typingBubble = appendChatMessage('ai', '<span class="typing-indicator">🧠 Pensando y explorando tu red neuronal...</span>');

  try {
    const res = await fetch('/api/ai/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messages: state.chatHistory,
        currentNoteName,
        currentNoteContent
      })
    });

    const data = await res.json();
    if (data.error) throw new Error(data.error);

    state.chatHistory.push({ role: 'assistant', content: data.reply });

    // Extract suggested links from chat reply to feed into gaps modal
    const rawChatLinks = [];
    const linkRx = /\[\[([^\]|#]+)\]\]/g;
    let lm;
    while ((lm = linkRx.exec(data.reply)) !== null) {
      const target = lm[1].trim();
      if (!rawChatLinks.includes(target)) rawChatLinks.push(target);
    }
    state.lastChatGaps = rawChatLinks;

    // Format reply with markdown and clickable obsidian links
    let formattedReply = data.reply;
    formattedReply = formattedReply.replace(/\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|([^\]]+))?\]\]/g, (match, link, alias) => {
      const label = alias || link;
      return `<a class="obsidian-link" onclick="openNoteByName('${link.trim()}')">🔗 [[${label.trim()}]]</a>`;
    });

    if (window.marked) {
      formattedReply = window.marked.parse(formattedReply);
    }

    typingBubble.querySelector('.bubble-content').innerHTML = `
      <div class="chat-reply-body">${formattedReply}</div>
      <div class="bubble-actions" style="display: flex; gap: 8px; flex-wrap: wrap;">
        <button class="btn btn-secondary btn-sm" onclick="createNoteFromChatResponse(\`${escapeForAttr(data.reply)}\`)">
          📝 Crear nota en Obsidian con esta respuesta
        </button>
        <button class="btn btn-magic btn-sm" onclick="openGapsModal(state.lastChatGaps)">
          🕵️ Resolver Lagunas con IA
        </button>
      </div>
    `;

    dom.chatMessages.scrollTop = dom.chatMessages.scrollHeight;
  } catch (err) {
    typingBubble.querySelector('.bubble-content').innerHTML = `<span style="color: #f43f5e">Error: ${err.message}</span>`;
  }
}

function appendChatMessage(role, contentHtml) {
  const bubble = document.createElement('div');
  bubble.className = `chat-bubble ${role}`;
  bubble.innerHTML = `
    <div class="bubble-avatar">${role === 'user' ? '👤' : '🧠'}</div>
    <div class="bubble-content">${contentHtml}</div>
  `;
  dom.chatMessages.appendChild(bubble);
  dom.chatMessages.scrollTop = dom.chatMessages.scrollHeight;
  return bubble;
}

window.createNoteFromChatResponse = function(rawContent) {
  // Extract a plausible title from first line or prompt
  const firstLine = rawContent.split('\n')[0].replace(/^[#*\s-]+/, '').slice(0, 40);
  const title = prompt('Título para la nueva nota en tu bóveda:', firstLine || 'Nota de Consulta IA');

  if (title) {
    dom.noteTitleInput.value = title.trim();
    dom.noteContentInput.value = rawContent;
    switchTab('editor');
    renderMarkdownPreview();
    updateStats();
    handleSaveNote();
  }
};

function escapeForAttr(str) {
  return str.replace(/`/g, '\\`').replace(/"/g, '&quot;');
}

// =========================================================
// Vault Grid (Explorador)
// =========================================================
function renderVaultGrid() {
  const query = dom.vaultSearchInput.value.toLowerCase().trim();
  const filtered = state.notes.filter(n => {
    return n.name.toLowerCase().includes(query) ||
      n.tags.some(t => t.toLowerCase().includes(query)) ||
      (n.excerpt && n.excerpt.toLowerCase().includes(query));
  });

  if (filtered.length === 0) {
    dom.vaultNotesGrid.innerHTML = `
      <div style="grid-column: 1 / -1; text-align: center; padding: 60px 20px; color: var(--text-muted);">
        <h3>No se encontraron notas</h3>
        <p>Escribe tu primera nota en el Editor Inteligente y pulsa <strong>Guardar en Obsidian</strong>.</p>
      </div>
    `;
    return;
  }

  dom.vaultNotesGrid.innerHTML = filtered.map(note => `
    <div class="vault-note-card" onclick="openNoteByName('${note.name}')">
      <div>
        <div class="note-card-title">
          <span>📄</span>
          <span>${note.name}</span>
        </div>
        <p class="note-card-excerpt">${note.excerpt || 'Sin contenido aún'}</p>
        <div class="tags-cloud" style="margin-bottom: 8px;">
          ${note.tags.map(t => `<span class="tag-pill">#${t}</span>`).join('')}
        </div>
      </div>
      <div class="note-card-footer">
        <span class="note-connections-badge">🔗 ${note.connectionCount || 0} conexiones</span>
        <span>${note.wordCount || 0} palabras</span>
      </div>
    </div>
  `).join('');
}

// =========================================================
// Settings Modal
// =========================================================
function openSettings() {
  dom.settingVaultPath.value = state.config.vaultPath;
  dom.settingModelSelect.value = state.config.currentModel;
  dom.settingsModal.classList.add('active');
}

function closeSettings() {
  dom.settingsModal.classList.remove('active');
}

async function handleSaveSettings() {
  const vaultPath = dom.settingVaultPath.value.trim();
  const groqKey = dom.settingGroqKey.value.trim();
  const model = dom.settingModelSelect.value;

  try {
    const res = await fetch('/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ vaultPath, groqKey, model })
    });

    const data = await res.json();
    if (data.error) throw new Error(data.error);

    state.config.vaultPath = data.vaultPath;
    state.config.currentModel = data.currentModel;
    dom.vaultPathDisplay.textContent = data.vaultPath;
    dom.modelSelect.value = data.currentModel;

    closeSettings();
    showToast('Configuración guardada correctamente', 'success');
    await loadNotes();
  } catch (err) {
    showToast('Error guardando configuración: ' + err.message, 'error');
  }
}

async function handleQuickModelChange() {
  const model = dom.modelSelect.value;
  try {
    await fetch('/api/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model })
    });
    state.config.currentModel = model;
    showToast(`Modelo cambiado a ${model}`, 'info');
  } catch (err) {
    showToast('Error cambiando modelo', 'error');
  }
}

// =========================================================
// 📚 Libro Maestro Controller
// =========================================================

let currentBookMarkdown = '';

async function loadBookStatsAndPreview() {
  try {
    const res = await fetch('/api/book/stats');
    const data = await res.json();
    if (data.exists) {
      if (dom.bookStatsChapters) dom.bookStatsChapters.textContent = data.totalChapters;
      if (dom.bookStatsWords) dom.bookStatsWords.textContent = (data.totalWordCount || 0).toLocaleString();
      if (dom.bookStatsPages) dom.bookStatsPages.textContent = `~${data.estimatedPages}`;
      if (dom.bookPagesBadge) dom.bookPagesBadge.textContent = `${data.estimatedPages}p`;
      if (dom.bookFilePathDisplay) dom.bookFilePathDisplay.textContent = data.filePath;
      if (dom.bookLastUpdated) {
        const d = new Date(data.updatedAt);
        dom.bookLastUpdated.textContent = `Actualizado: ${d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
      }

      currentBookMarkdown = data.markdown || '';
      renderBookPreview(currentBookMarkdown);
    } else {
      if (dom.bookReaderBody) {
        dom.bookReaderBody.innerHTML = `
          <div style="text-align: center; padding: 48px;">
            <p style="font-size: 16px; color: var(--text-secondary); margin-bottom: 16px;">
              El libro maestro aún no ha sido compilado.
            </p>
            <button class="btn btn-magic" onclick="handleCompileMasterBook()">
              ⚡ Compilar Libro Maestro Ahora
            </button>
          </div>
        `;
      }
    }
  } catch (err) {
    console.error('Error cargando estadísticas del libro:', err);
  }
}

function renderBookPreview(markdownText) {
  if (!dom.bookReaderBody) return;
  if (!markdownText) {
    dom.bookReaderBody.innerHTML = '<p style="color: var(--text-muted); padding: 24px;">No hay contenido disponible.</p>';
    return;
  }

  // Render markdown to HTML with marked
  let html = marked.parse(markdownText);

  // Convert [[wikilinks]] inside preview to clickable badges
  html = html.replace(/\[\[([^\]|#]+)(?:\|([^\]]+))?\]\]/g, (match, noteName, alias) => {
    const label = alias || noteName;
    return `<span class="obsidian-link" data-link="${noteName}" title="Ver nota en Obsidian">🔗 [[${label}]]</span>`;
  });

  dom.bookReaderBody.innerHTML = html;

  // Add click handler for wikilinks inside book preview
  dom.bookReaderBody.querySelectorAll('.obsidian-link').forEach(el => {
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      const targetNote = el.dataset.link;
      if (typeof openNoteByName === 'function') {
        openNoteByName(targetNote);
        switchTab('editor');
      }
    });
  });
}

async function handleCompileMasterBook() {
  const btn = dom.btnCompileMasterBook;
  setBtnLoading(btn, true, 'Compilando bóveda completa...');

  try {
    const res = await fetch('/api/book/compile', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ author: 'Luis' })
    });
    const data = await res.json();

    if (data.success) {
      showToast(`¡Libro actualizado con éxito! ${data.totalChapters} capítulos (~${data.estimatedPages} páginas)`, 'success');
      if (dom.bookStatsChapters) dom.bookStatsChapters.textContent = data.totalChapters;
      if (dom.bookStatsWords) dom.bookStatsWords.textContent = (data.totalWordCount || 0).toLocaleString();
      if (dom.bookStatsPages) dom.bookStatsPages.textContent = `~${data.estimatedPages}`;
      if (dom.bookPagesBadge) dom.bookPagesBadge.textContent = `${data.estimatedPages}p`;
      if (dom.bookLastUpdated) dom.bookLastUpdated.textContent = 'Actualizado: Ahora mismo';
      
      currentBookMarkdown = data.markdown;
      renderBookPreview(currentBookMarkdown);
      
      // Also refresh vault notes list in case user views it
      await loadNotes();
    } else {
      showToast(data.error || 'Error al compilar el libro', 'error');
    }
  } catch (err) {
    showToast(`Error al compilar: ${err.message}`, 'error');
  } finally {
    setBtnLoading(btn, false, 'Actualizar Libro con Toda mi Bóveda');
  }
}

function handlePrintBookPdf() {
  window.open('/book/print?auto=1', '_blank');
}

function handleOpenBookInEditor() {
  if (!currentBookMarkdown) {
    showToast('El libro maestro aún no se ha compilado.', 'warning');
    return;
  }
  
  state.currentNote = {
    title: '📚 Libro Maestro - Data Engineering de Cero a Senior',
    content: currentBookMarkdown,
    path: `${state.config.vaultPath}\\📚 Libro Maestro - Data Engineering de Cero a Senior.md`,
    links: [],
    tags: ['#libro-maestro', '#data-engineering'],
    backlinks: []
  };
  
  dom.noteTitleInput.value = state.currentNote.title;
  dom.noteContentInput.value = state.currentNote.content;
  updateStats();
  renderMarkdownPreview();
  switchTab('editor');
  showToast('Libro Maestro cargado en el editor de Markdown.', 'info');
}

// =========================================================
// Toast & Helpers
// =========================================================
function showToast(message, type = 'info') {
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span>${type === 'success' ? '✓' : type === 'error' ? '⚠️' : 'ℹ️'}</span>
    <span>${message}</span>
  `;
  dom.toastContainer.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

function setBtnLoading(btn, isLoading, text) {
  if (!btn) return;
  btn.disabled = isLoading;
  const lastSpan = btn.querySelector('span:last-child');
  if (lastSpan) {
    lastSpan.textContent = text;
  } else {
    btn.textContent = text;
  }
  btn.style.opacity = isLoading ? '0.7' : '1';
  btn.style.cursor = isLoading ? 'not-allowed' : 'pointer';
}

// Start
document.addEventListener('DOMContentLoaded', initApp);
