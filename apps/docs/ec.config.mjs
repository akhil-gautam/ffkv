import { defineEcConfig } from '@astrojs/starlight/expressive-code';

// Code frames use the same paper and night-garden surfaces as the home page.
// Function-valued overrides must live here, not inline in astro.config.mjs.
const pick = (dark, light) => ({ theme }) => (theme.type === 'dark' ? dark : light);

export default defineEcConfig({
  themes: ['github-dark', 'github-light'],
  styleOverrides: {
    borderRadius: '0.9rem',
    codeFontSize: '0.8125rem',
    codeFontFamily: "'Geist Mono Variable', ui-monospace, SFMono-Regular, Menlo, monospace",
    uiFontFamily: "'Geist Variable', ui-sans-serif, system-ui, sans-serif",
    codeBackground: pick('#11180c', '#fffdf8'),
    borderColor: pick('#26311f', '#e2dac9'),
    frames: {
      editorTabBarBackground: pick('#161e11', '#f3eee3'),
      editorActiveTabBackground: pick('#11180c', '#fffdf8'),
      terminalTitlebarBackground: pick('#161e11', '#f3eee3'),
      terminalBackground: pick('#11180c', '#fffdf8'),
      frameBoxShadowCssValue: 'none',
    },
  },
});
