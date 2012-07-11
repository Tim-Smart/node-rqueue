var rq      = require('../')

var q       = rq.createQueue(
  { name    : 'test'
  , prefix  : 'local:'
  , timeout : 5000
  }
)

q.id = 'TESTID'

q.once('data', function (job) {
  job.done()

  q.once('data', test2)
  q.write('test2')
})

function test2 (job) {
  job.emit('custom', 123)
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

q.subscribe('running')
q.on('running', function (job_id) {
  console.log('Got running', job_id)
})
q.subscribe('custom')
q.on('custom', function (job_id, data) {
  console.log('Got custom', job_id, data.toString())
})

q.listen()

setTimeout(
  function () {
    q.end()
  }
, 10000
)
