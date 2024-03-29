const {resolve} = require('path');
const webpack = require('webpack');

module.exports = {
  entry: {
    js: './src/app-client.js',
  },

  devtool: 'source-map',

  output: {
    path: resolve(__dirname, 'src', 'static', 'js'),
    filename: 'bundle.js',
  },

  resolve: {
    // Make src files outside of this dir resolve modules in our node_modules folder
    modules: [resolve(__dirname, '.'), resolve(__dirname, 'node_modules'), 'node_modules'],
    alias: {
      // From mapbox-gl-js README. Required for non-browserify bundlers (e.g. webpack):
      'mapbox-gl$': resolve('./node_modules/mapbox-gl/dist/mapbox-gl.js')
    }
  },

  module: {
    rules: [
      {
        test: /\.js$/,
        loader: 'babel-loader',
        include: [resolve('src'), resolve('node_modules/mapbox-gl/js'),]
      }
    ],
  },

  plugins: [
    new webpack.EnvironmentPlugin(['MAPBOX_ACCESS_TOKEN']),
    new webpack.DefinePlugin({
      "process.env": {
        NODE_ENV: JSON.stringify("production")
      }
    })
  ]
};
