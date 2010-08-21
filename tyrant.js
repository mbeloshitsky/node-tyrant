/*
 * Title: tyrant - tokyo tyrant to nodejs connector
 *
 * 2010, Michel Beloshitsky
 *
 * Licensed under the terms of MIT license. See COPYING file in the
 * root of distribution.
 */

var bin = require('./binary'), stream = require('./nstream')

/* Error codes */

var TTESUCCESS      = 0                /* success */
var TTEINVALID      = 1                /* invalid operation */
var TTENOHOST       = 2                /* host not found */
var TTEREFUSED      = 3                /* connection refused */
var TTESEND         = 4                /* send error */
var TTERECV         = 5                /* recv error */
var TTEKEEP         = 6                /* existing record */
var TTENOREC        = 7                /* no record found */
var TTEMISC         = 9999             /* miscellaneous error */

/* From ttutil.h */
var TTDEFPORT       = 1978              /* default port of the server */

var TTMAGICNUM      = 0xc8              /* magic number of each command */
var TTCMDPUT        = 0x10              /* ID of put command */
var TTCMDPUTKEEP    = 0x11              /* ID of putkeep command */
var TTCMDPUTCAT     = 0x12              /* ID of putcat command */
var TTCMDPUTSHL     = 0x13              /* ID of putshl command */
var TTCMDPUTNR      = 0x18              /* ID of putnr command */
var TTCMDOUT        = 0x20              /* ID of out command */
var TTCMDGET        = 0x30              /* ID of get command */
var TTCMDMGET       = 0x31              /* ID of mget command */
var TTCMDVSIZ       = 0x38              /* ID of vsiz command */
var TTCMDITERINIT   = 0x50              /* ID of iterinit command */
var TTCMDITERNEXT   = 0x51              /* ID of iternext command */
var TTCMDFWMKEYS    = 0x58              /* ID of fwmkeys command */
var TTCMDADDINT     = 0x60              /* ID of addint command */
var TTCMDADDDOUBLE  = 0x61              /* ID of adddouble command */
var TTCMDEXT        = 0x68              /* ID of ext command */
var TTCMDSYNC       = 0x70              /* ID of sync command */
var TTCMDOPTIMIZE   = 0x71              /* ID of optimize command */
var TTCMDVANISH     = 0x72              /* ID of vanish command */
var TTCMDCOPY       = 0x73              /* ID of copy command */
var TTCMDRESTORE    = 0x74              /* ID of restore command */
var TTCMDSETMST     = 0x78              /* ID of setmst command */
var TTCMDRNUM       = 0x80              /* ID of rnum command */
var TTCMDSIZE       = 0x81              /* ID of size command */
var TTCMDSTAT       = 0x88              /* ID of stat command */
var TTCMDMISC       = 0x90              /* ID of misc command */
var TTCMDREPL       = 0xa0              /* ID of repl command */

exports.make = function (host, port, wrkCount) {

   /*
     Topic: Workers management

     Here we use simple connection pool pattern. At the begin
     we make N connections and distribute it among incoming tasks.

     If number of incoming task requests overrates number of free connections
     we put task requests in fifo queue - which stored in wpending array.
   */

   /* Group: Workers handling */

   var workers = {}, free = [], busy = [], wpending = []

   /* Function: walloc
    * Allocate free connection or pend task request in case
    * of no free connections.
    */
   function walloc(cb) {
      var found = free.pop()
      if (found !== undefined) {
         busy.push(found)
         cb(null, workers[found])
      } else {
         wpending.push(cb)
      }
   }

   /* Function: wfree
    * Free connection and check pending queue. If queue contain
    * incoming task requests - alloc it immeditally. */
   function wfree(id) {
      busy = busy.reduce(function (res, x) {
         if (x != id) res.push(x); return res
      }, [])
      free.push(id)

      /* Check pending */
      while (wpending.length && free.length) {
         walloc(wpending.shift())
      }
   }

   /* Group: Public interface */
  
   /* Function: put
    * Put value into the storage
    */
   function put (k, v, mod, cb) {

      if (mod && !mod.toLowerCase)
         cb = mod, mod = null

      walloc(function (err, ts) {
         var jk = JSON.stringify(k), jv = JSON.stringify(v)

         var cmd = ({
            'keep': TTCMDPUTKEEP,
            'append': TTCMDPUTCAT
         })[mod] || (cb ? TTCMDPUT : TTCMDPUTNR)

         if (mod == 'append')
            jv = '+' + jv

         ts.s.write(
            bin.format('bbiiss', TTMAGICNUM, cmd, jk.length, jv.length, jk, jv),
            cmd == TTCMDPUTNR ? cb : null
         )

         if (cmd != TTCMDPUTNR)
            bin.read('b', ts.s, function (err, res) {
               cb(err || res[0])
               ts.free()
            })
      })
   }

   /* Function: get
    * Retreive value from database
    */
   function get (k, cb) {
      walloc(function (err, ts) {
         var jk = JSON.stringify(k)

         ts.s.write(
            bin.format('bbis', TTMAGICNUM, TTCMDGET, jk.length, jk)
         )

         bin.read('b', ts.s, function (err, res) {
            if (err || res[0]) {
               cb(err || res[0]); ts.free()
            } else {
               bin.read('S', ts.s, function (err, str) {
                  cb(err, eval(str[0])); ts.free()
               })
            }
         })

      })
   }

   /* Function:del
    * Delete value by key.
    */
   function del (k, cb) {
      var jk = JSON.stringify(k)
      walloc(function (err, ts) {

         ts.s.write(
            bin.format('bbis', TTMAGICNUM, TTCMDOUT, jk.length, jk)
         )

         bin.read('b', ts.s, function (err, res) {
            cb(err || res[0]); ts.free()
         })

      })
   }

   /* Function: iter
      Run iteration.
    */
   function iter(ks, ke, cb) {
      var jks = JSON.stringify(ks)

      walloc(function (err, ts) {

         function end (err) {
            cb(err, 'end')
            ts.free()
         }

         function next () {
            ts.s.write(
               bin.format('bbiiis', TTMAGICNUM, TTCMDMISC,
                          'iternext'.length, /* opts */0, /* argCount */0,
                          'iternext')
            )

            bin.read('bi', ts.s, function (err, res) {
               if (res[0]  == 1)
                  return end()
               if (err || res[0])
                  return end(err || res[0])
               bin.read('SS', ts.s, function (err, kv) {
                  var kj = eval(kv[0])

                  cb(null, 'k-v', kj, eval(kv[1]))

                  kj == ke ? end() : next()
               })
            })
         }

         ts.s.write(
            bin.format('bbiiis' + (jks ? 'is' : ''), TTMAGICNUM, TTCMDMISC,
                       'iterinit'.length, /* opts */0, /* argCount */jks ? 1 : 0,
                       'iterinit', jks.length, jks)
         )

         bin.read('bi', ts.s, function (err, res) {
            if (err || res[0])
               return cb(err || res[0])
            cb(null, 'start')
            next()
         })
      })
   }

   /* Function:halt
    * Close connection pool 
    */
   function halt() {
      halted = true
      for (w in workers) {
         workers[w].s.close()
      }
   }

   /* The Maker - makes connection pool. */

   wrkCount = wrkCount || 8 /* This default came from tyrant */

   while(wrkCount--) {

      /* Here we need a closure to make worker variable unique for
         each connection. */

      (function () {
         var id = wrkCount, worker = {
            s:    stream.make(port || TTDEFPORT, host, 'binary'),
            free: function () { wfree(id) }
         }

         worker.s.on('connect', function () { worker.free() })

         workers[id]=worker
         busy.push(id)
      })()
   }

   /* Return public interface */

   return {put: put, get: get, del: del, iter:iter, halt: halt}

}
