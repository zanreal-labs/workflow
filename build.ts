import dts from 'bun-plugin-dts'

await Bun.build({
  entrypoints: ['./src/index.ts'],
  outdir: './dist',
  plugins: [
    dts()
  ],
  splitting: true,
  minify: true,
  target: 'node'
})