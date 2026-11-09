// Canvas dot art for the home page, in the four core colours.
//
//   swarm    (hero)   Client requests drift in from the left and split into four
//                     per-core clusters that swell with the traffic they receive.
//   persist  (footer) Writes sink through a halftone that thickens into the dark
//                     footer, like data settling into the write-ahead log.
//
// Both pause off-screen and in background tabs, and draw one still frame under
// prefers-reduced-motion.

type Palette = { cores: string[]; night: string };
type Renderer = { resize(w: number, h: number): void; frame(t: number, dt: number): void };
type Factory = (ctx: CanvasRenderingContext2D, palette: Palette) => Renderer;

const rgba = (hex: string, alpha: number) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${n >> 16} ${(n >> 8) & 255} ${n & 255} / ${alpha})`;
};

const swarm: Factory = (ctx, palette) => {
  type Particle = { x: number; y: number; vx: number; vy: number; core: number; r: number };
  let width = 0, height = 0, cores: { x: number; y: number; load: number }[] = [];
  let particles: Particle[] = [];
  const spawn = (): Particle => ({
    x: -10 - Math.random() * 80, y: height * (0.4 + Math.random() * 0.52),
    vx: 1.4 + Math.random() * 1.4, vy: 0, core: (Math.random() * 4) | 0, r: 1.8 + Math.random() * 2,
  });
  return {
    resize(w, h) {
      width = w; height = h;
      cores = [0, 1, 2, 3].map((i) => ({ x: w * (0.56 + i * 0.12), y: h * (0.52 + (i % 2) * 0.2), load: 0 }));
      particles = Array.from({ length: Math.round(w / 1.5) }, () => {
        const p = spawn();
        p.x = Math.random() * w * 0.7;
        return p;
      });
    },
    frame(t, dt) {
      const steps = dt / 16.7, s = t / 1000;
      ctx.clearRect(0, 0, width, height);
      for (const core of cores) core.load *= Math.pow(0.97, steps);
      for (const p of particles) {
        const target = cores[p.core];
        const dx = target.x - p.x, dy = target.y - p.y, dist = Math.hypot(dx, dy);
        // Wander on a soft flow field, then steer toward the owning core.
        const steer = Math.min(1, Math.max(0, (p.x / width - 0.15) * 1.6));
        p.vy += (Math.sin(p.x * 0.012 + s) * 0.06 + (dy / dist) * 0.22 * steer) * steps;
        p.vx += (dx / dist) * 0.12 * steer * steps;
        p.vx = Math.max(p.vx * Math.pow(0.985, steps), 0.8);
        p.vy *= Math.pow(0.9, steps);
        p.x += p.vx * steps * 1.6; p.y += p.vy * steps * 1.6;
        if (dist < 18) {
          target.load = Math.min(1, target.load + 0.035);
          Object.assign(p, spawn());
          continue;
        }
        const near = 1 - Math.min(1, dist / (width * 0.5));
        ctx.fillStyle = rgba(palette.cores[p.core], 0.3 + near * 0.7);
        ctx.beginPath(); ctx.arc(p.x, p.y, p.r * (0.7 + near * 0.6), 0, Math.PI * 2); ctx.fill();
      }
      // Each core is a packed disc of dots that swells with its load.
      cores.forEach((core, i) => {
        const radius = 22 + core.load * 16;
        ctx.fillStyle = palette.cores[i];
        for (let ring = 0; ring * 7 < radius; ring++) {
          const n = Math.max(1, Math.round((ring * 7 * Math.PI * 2) / 8));
          for (let k = 0; k < n; k++) {
            const a = (k / n) * Math.PI * 2 + s * (i % 2 ? 0.4 : -0.4) * (ring % 2 ? 1 : -1);
            ctx.beginPath(); ctx.arc(core.x + Math.cos(a) * ring * 7, core.y + Math.sin(a) * ring * 7, 2.6, 0, Math.PI * 2); ctx.fill();
          }
        }
        ctx.strokeStyle = rgba(palette.cores[i], 0.35);
        ctx.setLineDash([2, 5]); ctx.lineWidth = 1.5;
        ctx.beginPath(); ctx.arc(core.x, core.y, radius + 10, 0, Math.PI * 2); ctx.stroke();
        ctx.setLineDash([]);
      });
    },
  };
};

const persist: Factory = (ctx, palette) => {
  type Drop = { x: number; y: number; speed: number; core: number; r: number; phase: number };
  const pitch = 10;
  let width = 0, height = 0, halftone: HTMLCanvasElement | null = null;
  let drops: Drop[] = [];
  const spawn = (y: number): Drop => ({
    x: Math.random() * width, y, speed: 0.25 + Math.random() * 0.5,
    core: (Math.random() * 4) | 0, r: 3 + Math.random() * 2.2, phase: Math.random() * 6.3,
  });
  return {
    resize(w, h) {
      width = w; height = h;
      // Rows of night-coloured dots grow downward until they merge into the footer.
      halftone = document.createElement('canvas');
      const ratio = devicePixelRatio || 1;
      halftone.width = Math.round(w * ratio); halftone.height = Math.round(h * ratio);
      const layer = halftone.getContext('2d')!;
      layer.scale(ratio, ratio);
      layer.fillStyle = palette.night;
      // Hex-packed rows; dots reach pitch/√3 (fully closed) at the solid edge.
      const rowGap = pitch * 0.866, solid = h * 0.86;
      for (let row = 0, y = rowGap / 2; y < solid + rowGap; row++, y += rowGap) {
        const depth = Math.min(1, y / solid);
        const r = 0.5 + (pitch * 0.62 - 0.5) * Math.pow(depth, 2.1);
        const shift = row % 2 ? pitch / 2 : 0;
        layer.beginPath();
        for (let x = shift; x < w + pitch; x += pitch) { layer.moveTo(x + r, y); layer.arc(x, y, r, 0, Math.PI * 2); }
        layer.fill();
      }
      layer.fillRect(0, solid, w, h - solid);
      drops = Array.from({ length: Math.round(w / 12) }, () => spawn(Math.random() * h * 0.8));
    },
    frame(t, dt) {
      const steps = dt / 16.7, s = t / 1000;
      ctx.clearRect(0, 0, width, height);
      if (halftone) ctx.drawImage(halftone, 0, 0, width, height);
      for (const drop of drops) {
        drop.y += drop.speed * steps;
        const depth = drop.y / height;
        if (depth > 0.86) Object.assign(drop, spawn(-8));
        // Writes shrink as they sink into the log; they glow brightest near the top.
        const r = drop.r * (1 - 0.75 * Math.pow(Math.max(0, depth), 1.6));
        ctx.fillStyle = rgba(palette.cores[drop.core], 0.95 - depth * 0.35);
        ctx.beginPath(); ctx.arc(drop.x + Math.sin(s * 1.3 + drop.phase) * 2, drop.y, Math.max(0.6, r), 0, Math.PI * 2); ctx.fill();
      }
    },
  };
};

const FACTORIES = { swarm, persist };

/** Runs one dot-art renderer on `canvas` while it is visible. */
export function startDotArt(canvas: HTMLCanvasElement, kind: keyof typeof FACTORIES) {
  const ctx = canvas.getContext('2d')!;
  const css = getComputedStyle(document.documentElement);
  const palette: Palette = {
    cores: ['--amber', '--coral', '--lime', '--sky'].map((name) => css.getPropertyValue(name).trim()),
    night: css.getPropertyValue('--night').trim(),
  };
  const renderer = FACTORIES[kind](ctx, palette);
  const still = matchMedia('(prefers-reduced-motion: reduce)').matches;
  let running = false, frame = 0, last = 0;
  const size = () => {
    const { width, height } = canvas.getBoundingClientRect();
    const ratio = devicePixelRatio || 1;
    canvas.width = Math.round(width * ratio); canvas.height = Math.round(height * ratio);
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    renderer.resize(width, height);
    if (!running) renderer.frame(performance.now(), 16.7);
  };
  const tick = (now: number) => {
    renderer.frame(now, Math.min(50, now - (last || now)));
    last = now;
    if (running) frame = requestAnimationFrame(tick);
  };
  const toggle = (on: boolean) => {
    if (still || on === running) return;
    running = on; last = 0;
    if (on) frame = requestAnimationFrame(tick); else cancelAnimationFrame(frame);
  };
  size();
  addEventListener('resize', size, { passive: true });
  let visible = false;
  new IntersectionObserver(([entry]) => { visible = entry.isIntersecting; toggle(visible && !document.hidden); }).observe(canvas);
  document.addEventListener('visibilitychange', () => toggle(visible && !document.hidden));
}
