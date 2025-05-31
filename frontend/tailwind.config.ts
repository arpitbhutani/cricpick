import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      backgroundImage: {
        "gradient-radial": "radial-gradient(var(--tw-gradient-stops))",
        "gradient-conic":
          "conic-gradient(from 180deg at 50% 50%, var(--tw-gradient-stops))",
      },
      colors: {
        primary: '#1A202C', // Dark Blue/Gray (example)
        secondary: '#2D3748', // Slightly lighter dark blue/gray
        accent: '#3182CE', // Blue (example)
        highlight: '#4299E1', // Lighter Blue
        background: '#F7FAFC', // Very light gray (almost white)
        card: '#FFFFFF',
        textPrimary: '#1A202C',
        textSecondary: '#A0AEC0',
      }
    },
  },
  plugins: [],
};
export default config; 