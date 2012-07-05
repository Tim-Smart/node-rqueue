var rq      = require('../')

var q       = rq.createQueue(
  { name    : 'test'
  , prefix  : 'local:'
  //, timeout : 5000
  }
)

q.id = 'TESTID'

q.once('data', function (job) {
  job.done()

  q.once('data', test2)
  q.write('test2')
})

function test2 (job) {
  job.retry()
  q.once('data', test3)
}

function test3 (job) {
  job.done()
  q.write('test3')
  q.getJob(job.id, function (error, job) {
    console.log(job.payload)
  })
}

q.write('test')

q.listen()

setTimeout(
  function () {
    q.end()
  }
, 10000
)
