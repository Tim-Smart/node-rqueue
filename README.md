node-rqueue
-----------

node-rqueue is a simple Redis based work queue for communicating between multiple platforms.


Usage
-----

```javascript
var rq      = require('rqueue')
var assert  = require('assert').ok
var q       = q.createQueue(
  { host    : '127.0.0.1'
  , port    : 6379
  , auth    : 'redis-password'
  , name    : 'superq'
  , prefix  : 'local:'
  , timeout : 5000
  }
)

// Start the worker
q.listen()
console.log('Worker', q.id, 'started')

// Listen for jobs. Emits rq.Job objects
q.on('data', function (job) {
  console.log('Processing job', job.id)
  job.done() // Move to `completed` status
  // or
  // job.retry() // Re-queue job
  // job.done(error) // Move job to `failed` status
  // job.retry(error)

  // Emit custom events on pubsub
  job.emit('progress', 100)

  // Listen for events with a redis client:
  // redis.psubscribe('local:rq:superq:progress:*')
  // redis.on('pmessage:local:rq:superq:progress:*', function (key) {
  //   // Key is `local:rq:superq:progress:*job.id*`
  //   var job_id = key.split(':').pop()
  //   q.getJob(id, gotJob)
  // })
  //
  // rqueue emits a lot of events for you. The job id is always the message.
  // A somewhat exhaustive list:
  // (Prefix all these with: q.prefix + ':rq:' + q.name + ':'. For the above
  //  queue it would be `local:rq:superq:`)
  //
  // * Everytime a job changes (job.forward) status, a event named after the
  //   new status is emitted. E.g.
  //
  //   * `queued`, `failed`, `complete`, `timeout`, `running`
  //
  // * Everytime a job is saved, a `save` event is emitted.
  //   If it is a new job, `new` is emitted as well.
  //
  // * Everytime a job is removed, a `remove` event is emitted.

  // IMPORTANT if you want the worker registration / deregistration to work
  // as expected.
  q.end()

  // If a worker has failed or crashed, the id will be in
  // `local:rq:superq:workers` and its unfinished running jobs
  // will be in `local:rq:superq:running:*workerid*`
  // You can migrate the unfinished worker by:
  //
  // q.migrate('workerid', callback)
})

// Quickly create a job and push it to 'queued'
q.write({ my : 'data' })
// Callback can be used as second argument.

// Create a job and mess with individual options.
var job = q.job({ my : 'data' })
assert(null === job.status)
assert(0 === job.retries)

// Timeouts
assert(5000 === job.timeout) // Set from default timeout in queue options
job.timeout = 10000 // 10 seconds

// Priority. Higher is more important. Default is 1
assert(1 === job.priority)
job.priority = 9001 // Over 9000

// Add some messages
job.message('Custom info message')
job.error(new Error('fail'))

// Save the job and push it to the 'queued' status
job.forward('queued', function (error) {
  if (error) throw error

  // Get a job from an id
  q.findJob(job.id, function (error, job) {})
})
assert('queued' === job.status)

// Or save it for forwarding later
job.save(function (error) {})
```


Implement in another language
-----------------------------

TODO
