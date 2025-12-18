const svg = document.getElementById('svg');
const stage = document.getElementById('stage');

// Original content size
const CONTENT_WIDTH = 1200;
const CONTENT_HEIGHT = 800;

// Current viewBox
let viewBox = { x: 0, y: 0, width: CONTENT_WIDTH, height: CONTENT_HEIGHT };
const minScaleDefault = 0.4;
let minScaleFloor = minScaleDefault; // dynamic lower bound based on actual viewBox

function syncViewBoxFromSvg() {
    if (!svg) return;

    const liveViewBox = svg.viewBox?.baseVal;
    if (liveViewBox && liveViewBox.width && liveViewBox.height) {
        viewBox = {
            x: liveViewBox.x,
            y: liveViewBox.y,
            width: liveViewBox.width,
            height: liveViewBox.height
        };
        return;
    }

    const attr = svg.getAttribute('viewBox');
    if (!attr) return;

    const parts = attr.trim().split(/[\s,]+/).map(Number);
    if (parts.length !== 4 || parts.some(n => Number.isNaN(n))) {
        return;
    }

    viewBox = {
        x: parts[0],
        y: parts[1],
        width: parts[2],
        height: parts[3]
    };

    // Track the smallest scale we've seen so wheel zoom won't snap to a higher minimum
    const scaleX = CONTENT_WIDTH / viewBox.width;
    const scaleY = CONTENT_HEIGHT / viewBox.height;
    const baselineScale = Math.min(scaleX, scaleY);
    minScaleFloor = Math.min(minScaleFloor, baselineScale);
}

syncViewBoxFromSvg();

const maxScale = 20;

// Panning & Pinching state
let isPanning = false;
let panStart = { x: 0, y: 0 };
let viewAtPanStart = { x: 0, y: 0 };
let isPinching = false;
let touchDistance = 0;
let touchCenter = { x: 0, y: 0 };

// -----------------------------
// Initialize: Fit viewBox to container
// -----------------------------

function fitToScreen() {
    const rect = stage.getBoundingClientRect();

    // Safety: wait for layout
    if (!rect.width || !rect.height) {
        setTimeout(fitToScreen, 50);
        return;
    }

    const containerAspect = rect.width / rect.height;
    const contentAspect = 1200 / 800; // original SVG

    if (containerAspect < contentAspect) {
        // Portrait: fit width, center vertically
        viewBox = {
            x: 0,
            y: (800 - (1200 / containerAspect)) / 2,
            width: 1200,
            height: 1200 / containerAspect
        };
    } else {
        // Landscape: fit height, center horizontally
        viewBox = {
            x: (1200 - (800 * containerAspect)) / 2,
            y: 0,
            width: 800 * containerAspect,
            height: 800
        };
    }

    updateViewBox();
}

function updateViewBox() {
    svg.setAttribute('viewBox', `${viewBox.x} ${viewBox.y} ${viewBox.width} ${viewBox.height}`);
}

// Call after load, with delay for mobile layout stability
window.addEventListener('load', () => {
    setTimeout(fitToScreen, 100);
});

// On resize and orientation change
window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(fitToScreen, 100);
});

let resizeTimeout;

// Critical: on mobile rotate
window.addEventListener('orientationchange', () => {
    setTimeout(fitToScreen, 150);
});

// -----------------------------
// Mouse Wheel Zoom
// -----------------------------
stage.addEventListener('wheel', (e) => {
    e.preventDefault();
    zoom({ x: e.clientX, y: e.clientY }, -e.deltaY);
}, { passive: false });

function zoom(point, delta) {
    syncViewBoxFromSvg();

    const currentScale = Math.min(
        CONTENT_WIDTH / viewBox.width,
        CONTENT_HEIGHT / viewBox.height
    );
    const minAllowedScale = Math.min(minScaleDefault, minScaleFloor);
    const scaleMultiplier = delta > 0 ? 1.1 : 1 / 1.1;
    let newScale = currentScale * scaleMultiplier;
    newScale = Math.min(Math.max(newScale, minAllowedScale), maxScale);

    const newWidth = CONTENT_WIDTH / newScale;
    const newHeight = CONTENT_HEIGHT / newScale;

    const rect = stage.getBoundingClientRect();
    const offsetX = (point.x - rect.left);
    const offsetY = (point.y - rect.top);

    const relX = offsetX / rect.width;
    const relY = offsetY / rect.height;

    viewBox.x = viewBox.x + relX * (viewBox.width - newWidth);
    viewBox.y = viewBox.y + relY * (viewBox.height - newHeight);
    viewBox.width = newWidth;
    viewBox.height = newHeight;

    updateViewBox();
}

// -----------------------------
// Mouse Panning
// -----------------------------
stage.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    e.preventDefault();

    isPanning = true;
    syncViewBoxFromSvg();
    panStart = { x: e.clientX, y: e.clientY };
    viewAtPanStart = { ...viewBox };

    stage.style.cursor = 'grabbing';
    document.body.style.userSelect = 'none';
});

document.addEventListener('mousemove', (e) => {
    if (!isPanning) return;
    e.preventDefault();

    const dx = e.clientX - panStart.x;
    const dy = e.clientY - panStart.y;

    const rect = stage.getBoundingClientRect();
    const scaleX = viewBox.width / rect.width;
    const scaleY = viewBox.height / rect.height;

    viewBox.x = viewAtPanStart.x - dx * scaleX;
    viewBox.y = viewAtPanStart.y - dy * scaleY;

    updateViewBox();
});

document.addEventListener('mouseup', (e) => {
    if (e.button !== 0) return;
    if (isPanning) {
        isPanning = false;
        stage.style.cursor = '';
        document.body.style.userSelect = '';
    }
});

stage.addEventListener('contextmenu', (e) => e.preventDefault());

// -----------------------------
// Touch: One-finger pan + Two-finger pinch
// -----------------------------

let touchStart = { x: 0, y: 0 };
let isSingleTouch = false;

stage.addEventListener('touchstart', (e) => {
    syncViewBoxFromSvg();
    // Reset states
    if (e.touches.length === 1) {
        // One finger: prepare for panning
        isSingleTouch = true;
        const touch = e.touches[0];
        touchStart = { x: touch.clientX, y: touch.clientY };
        viewAtPanStart = { ...viewBox };

        // Optional: show grabbing cursor if needed (not visible on mobile anyway)
    } else if (e.touches.length === 2) {
        // Two fingers: prepare for pinch zoom
        isPinching = true;
        const [t1, t2] = e.touches;
        const dx = t1.clientX - t2.clientX;
        const dy = t1.clientY - t2.clientY;
        touchDistance = Math.hypot(dx, dy);
        touchCenter = {
            x: (t1.clientX + t2.clientX) / 2,
            y: (t1.clientY + t2.clientY) / 2
        };
    }
    // Prevent ghost events
    e.preventDefault();
}, { passive: false });

stage.addEventListener('touchmove', (e) => {
    if (e.touches.length === 1 && isSingleTouch) {
        e.preventDefault();

        const touch = e.touches[0];
        const dx = touch.clientX - touchStart.x;
        const dy = touch.clientY - touchStart.y;

        // Critical: scale movement by current zoom level
        const rect = stage.getBoundingClientRect();
        const scaleX = viewBox.width / rect.width;
        const scaleY = viewBox.height / rect.height;

        // This is the key: move the viewBox by the *equivalent content distance*
        viewBox.x = viewAtPanStart.x - dx * scaleX;
        viewBox.y = viewAtPanStart.y - dy * scaleY;

        updateViewBox();
    }
    else if (e.touches.length === 2) {
        // Two fingers = pinch zoom
        e.preventDefault();
        isSingleTouch = false;
        isPinching = true;

        const [t1, t2] = e.touches;
        const dx = t1.clientX - t2.clientX;
        const dy = t1.clientY - t2.clientY;
        const distance = Math.hypot(dx, dy);

        const delta = distance - touchDistance;
        if (Math.abs(delta) > 4) { // threshold to avoid jitter
            zoom(touchCenter, delta);
            touchDistance = distance;
            // Recenter after zoom for smoother feel
            touchCenter = {
                x: (t1.clientX + t2.clientX) / 2,
                y: (t1.clientY + t2.clientY) / 2
            };
        }
    }
}, { passive: false });

stage.addEventListener('touchend', (e) => {
    // Reset states based on remaining touches
    if (e.touches.length < 2) {
        isPinching = false;
    }
    if (e.touches.length === 0) {
        // All fingers up
        isSingleTouch = false;
    }
});

window.addEventListener('load', () => {
    const nodes = document.querySelectorAll('.node');

    nodes.forEach(node => {
        node.addEventListener('mouseenter', () => {
            const label = document.getElementById(node.dataset.labelId);
            if (label) label.style.display = 'inline';
        });
        node.addEventListener('mouseleave', () => {
            const label = document.getElementById(node.dataset.labelId);
            if (label) label.style.display = 'none';
        });
    });
});
