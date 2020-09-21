const { Marp } = require('@marp-team/marp-core')
const MarkdownIt = require('markdown-it')
const highlightLines = require('markdown-it-highlight-lines')

const md = new MarkdownIt()
md.use(highlightLines)



module.exports = (opts) => new Marp(opts).use(highlightLines)
