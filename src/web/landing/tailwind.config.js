/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./assets/**/*.js",
    "./templates/**/*.html.twig",
  ],
  theme: {
    extend: {
      colors: {
        black: "#1b1925",
        gray: "#323232",
        orange: {
          100: "#ff5547",
          200: "#DB4B3D",
          300: "#B33D32",
        },
        blue: {
          100: "#806dfe",
          200: "#5945d8",
          300: "#4026ac",
        },
        brown: {
          100: "#e1761a",
          200: "#ad6126",
          300: "#8c4716",
        }
      },
    },
  },
  plugins: [],
}
