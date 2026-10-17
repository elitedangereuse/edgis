(function () {
    'use strict';

    // This module is conditionally requested by sysmap.html only for
    // ARCHITECT=1. Keep all early-access UI and state in this module.
    if (!globalThis.EDGIS_SYSMAP_CONFIG?.architectModeEnabled) {
        return;
    }

    let enabled = false;

    const updateArchitectMode = (button) => {
        document.body.classList.toggle('architect-mode', enabled);
        button.classList.toggle('active', enabled);
        button.setAttribute('aria-pressed', String(enabled));
        button.setAttribute('title', enabled ? 'Exit Architect View' : 'Enter Architect View');
        button.setAttribute('aria-label', button.title);
        globalThis.dispatchEvent(new CustomEvent('edgis:architect-mode-change', {
            detail: { enabled },
        }));
    };

    const addArchitectButton = () => {
        const toolbar = document.querySelector('.toolbar');
        if (!toolbar || document.getElementById('architectModeButton')) {
            return;
        }

        const button = document.createElement('button');
        button.id = 'architectModeButton';
        button.type = 'button';
        button.setAttribute('aria-pressed', 'false');
        button.innerHTML = `
            <svg width="26" height="26" viewBox="0 0 24 24" aria-hidden="true">
              <path d="M8 3v15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
              <path d="M9 4h8v7l-4-2.5L9 11z" fill="currentColor"/>
              <path d="M4 21a8 8 0 0 1 16 0" fill="none" stroke="currentColor" stroke-width="1.75"/>
              <path d="M12 17v4" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round"/>
            </svg>`;
        button.addEventListener('click', () => {
            enabled = !enabled;
            updateArchitectMode(button);
        });
        toolbar.appendChild(button);
        updateArchitectMode(button);
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', addArchitectButton, { once: true });
    } else {
        addArchitectButton();
    }
}());
