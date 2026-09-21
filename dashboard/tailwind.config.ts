import type { Config } from "tailwindcss"

const config = {
  darkMode: ["class"],
  // `hover:` only on devices that can hover, so taps don't leave sticky hover
  // styles on phones. Safe here: no element is revealed only on hover.
  future: { hoverOnlyWhenSupported: true },
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  prefix: "",
  theme: {
  	container: {
  		center: true,
  		padding: '2rem',
  		screens: {
  			'2xl': '1400px'
  		}
  	},
  	extend: {
  		fontFamily: {
  			sans: ['var(--font-jakarta)', 'system-ui', 'sans-serif'],
  			mono: ['var(--font-jetbrains)', 'Fira Code', 'monospace'],
  			display: ['var(--font-outfit)', 'var(--font-jakarta)', 'sans-serif'],
  		},
  		colors: {
  			border: 'hsl(var(--border))',
  			input: 'hsl(var(--input))',
  			ring: 'hsl(var(--ring))',
  			background: 'hsl(var(--background))',
  			foreground: 'hsl(var(--foreground))',
  			primary: {
  				DEFAULT: 'hsl(var(--primary))',
  				foreground: 'hsl(var(--primary-foreground))'
  			},
  			secondary: {
  				DEFAULT: 'hsl(var(--secondary))',
  				foreground: 'hsl(var(--secondary-foreground))'
  			},
  			tertiary: {
  				DEFAULT: 'hsl(var(--tertiary))',
  				foreground: 'hsl(var(--tertiary-foreground))'
  			},
  			destructive: {
  				DEFAULT: 'hsl(var(--destructive))',
  				foreground: 'hsl(var(--destructive-foreground))'
  			},
  			muted: {
  				DEFAULT: 'hsl(var(--muted))',
  				foreground: 'hsl(var(--muted-foreground))'
  			},
  			accent: {
  				DEFAULT: 'hsl(var(--accent))',
  				foreground: 'hsl(var(--accent-foreground))'
  			},
  			popover: {
  				DEFAULT: 'hsl(var(--popover))',
  				foreground: 'hsl(var(--popover-foreground))'
  			},
  			card: {
  				DEFAULT: 'hsl(var(--card))',
  				foreground: 'hsl(var(--card-foreground))'
  			},
  			chart: {
  				'1': 'hsl(var(--chart-1))',
  				'2': 'hsl(var(--chart-2))',
  				'3': 'hsl(var(--chart-3))',
  				'4': 'hsl(var(--chart-4))',
  				'5': 'hsl(var(--chart-5))'
  			},
  			status: {
  				win: 'hsl(var(--status-win))',
  				loss: 'hsl(var(--status-loss))',
  				projected: 'hsl(var(--status-projected))',
  				neutral: 'hsl(var(--status-neutral))'
  			},
  			signal: {
  				hot: 'hsl(var(--signal-hot))',
  				cold: 'hsl(var(--signal-cold))',
  				live: 'hsl(var(--signal-live))',
  				stale: 'hsl(var(--signal-stale))'
  			}
  		},
  		borderRadius: {
  			lg: 'var(--radius)',
  			md: 'calc(var(--radius) - 2px)',
  			sm: 'calc(var(--radius) - 4px)'
  		},
  		keyframes: {
  			'caret-blink': {
  				'0%,70%,100%': {
  					opacity: '1'
  				},
  				'20%,50%': {
  					opacity: '0'
  				}
  			},
  			'accordion-down': {
  				from: {
  					height: '0'
  				},
  				to: {
  					height: 'var(--radix-accordion-content-height)'
  				}
  			},
  			'accordion-up': {
  				from: {
  					height: 'var(--radix-accordion-content-height)'
  				},
  				to: {
  					height: '0'
  				}
  			},
  			scroll: {
  				to: {
  					transform: 'translate(calc(-50% - 0.5rem))'
  				}
  			},
  			'glow-pulse': {
  				'0%, 100%': {
  					boxShadow: '0 0 0 0 hsl(var(--primary) / 0)'
  				},
  				'50%': {
  					boxShadow: '0 0 20px 2px hsl(var(--primary) / 0.3)'
  				}
  			},
  			'slide-up-fade': {
  				'0%': {
  					opacity: '0',
  					transform: 'translateY(8px)'
  				},
  				'100%': {
  					opacity: '1',
  					transform: 'translateY(0)'
  				}
  			},
  			'scale-in': {
  				'0%': {
  					opacity: '0',
  					transform: 'scale(0.95)'
  				},
  				'100%': {
  					opacity: '1',
  					transform: 'scale(1)'
  				}
  			},
  			'data-flash': {
  				'0%': {
  					backgroundColor: 'hsl(28 92% 52% / 0.12)'
  				},
  				'100%': {
  					backgroundColor: 'transparent'
  				}
  			},
  			'beacon': {
  				'0%, 100%': {
  					opacity: '0.4'
  				},
  				'50%': {
  					opacity: '1'
  				}
  			},
  			'scanline': {
  				'0%': {
  					transform: 'translateY(-100%)'
  				},
  				'100%': {
  					transform: 'translateY(100vh)'
  				}
  			},
  			'ticker-roll': {
  				'0%': {
  					transform: 'translateY(-100%)',
  					opacity: '0'
  				},
  				'50%': {
  					opacity: '1'
  				},
  				'100%': {
  					transform: 'translateY(0)',
  					opacity: '1'
  				}
  			}
  		},
  		animation: {
  			scroll: 'scroll var(--animation-duration, 40s) var(--animation-direction, forwards) linear infinite',
  			'accordion-down': 'accordion-down 0.2s ease-out',
  			'accordion-up': 'accordion-up 0.2s ease-out',
  			'caret-blink': 'caret-blink 1.25s ease-out infinite',
  			'glow-pulse': 'glow-pulse 2s ease-in-out infinite',
  			'slide-up-fade': 'slide-up-fade 0.3s ease-out',
  			'scale-in': 'scale-in 0.2s ease-out',
  			'data-flash': 'data-flash 0.6s ease-out',
  			'beacon': 'beacon 2s ease-in-out infinite',
  			'scanline': 'scanline 8s linear infinite',
  			'ticker-roll': 'ticker-roll 0.3s ease-out'
  		}
  	}
  },
  plugins: [require("tailwindcss-animate"), addVariablesForColors],
} satisfies Config

export default config

const colors = require("tailwindcss/colors");
const {
  default: flattenColorPalette,
} = require("tailwindcss/lib/util/flattenColorPalette");


function addVariablesForColors({ addBase, theme }: any) {
  let allColors = flattenColorPalette(theme("colors"));
  let newVars = Object.fromEntries(
    Object.entries(allColors).map(([key, val]) => [`--${key}`, val])
  );

  addBase({
    ":root": newVars,
  });
}
