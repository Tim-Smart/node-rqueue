var rq      = require('../')

var q       = rq.createQueue(
  { name    : 'test'
  , prefix  : 'local:'
  //, timeout : 5000
  }
)

q.migrate('TESTID', function () {
  q.end()
})
