/**
 * Tokyo tyrant connectior
 * 
 * 2010, Michel Beloshitsky
 *
 * Licensed under the terms of MIT license. See COPYING file in the
 * root of distribution.
 */

var net = require('net'), sys = require('sys'), bin = require('./binary')

/* From tcrdb.h */
/* enumeration for error codes */
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

   /* State */
   var halted = false

   /* Workers management */
   var workers = {}, free = [], busy = [], wpending = []

   function walloc(cb) {
      var found = free.pop()
      if (found !== undefined) {
         busy.push(found)
         cb(null, workers[found])
      } else {
         wpending.push(cb)
      }
   }

   function wfree(id) {
      busy = busy.reduce(function (res, x) {
         if (x != id) res.push(x); return res
      }, [])
      free.push(id)
      while (wpending.length && free.length) {
         walloc(wpending.shift())
      }
   }

   /* TCP connection events handling */
   function onConnect(state) {
      state.stream.setEncoding('binary')
      wfree(state.id)
   }

   function onDrain(state) {
      /* Possible not correct */
      if (!state.cmd)
         return

      switch (state.cmd) {
      case TTCMDPUTNR:
         fin(state)
         break
      }
   }

   function onData(state, data) {
      /* Possible not correct */
      if (!state.cmd)
         return

      switch (state.cmd) {
      case TTCMDPUT:
      case TTCMDOUT:
      case TTCMDPUTKEEP:
      case TTCMDPUTCAT:
         var ed = bin.read('b', data), err = ed[1][0], data = ed[0]
         if (err != TTESUCCESS) {
            fin(state, {code:err}, data)
         }
         fin(state, null, data)
         break
      case TTCMDGET:
         if (!state.rem) {
            var ed = bin.read('bI', data), err = ed[1][0], len = ed[1][1], data = ed[0]
            if (err != TTESUCCESS) {
               fin(state, {code:err}, data)
            }
            state.rem = ['', len]
         }
         if (state.rem) {
            var len = state.rem[1]
            var plen = len - state.rem[0].length
            state.rem[0] += data.slice(0, plen)
            if (len == state.rem[0].length) {
               fin(state, null, eval(state.rem[0]))
	       delete state.rem
            }
         }
         break
      case TTCMDMISC:
         var ed = bin.read('bI', data), err = ed[1][0], data = ed[0], len = ed[1][1]
	 if (state.miscCmd == 'iterinit') {
            if (err != TTESUCCESS) {
               return fin(state, {code:err}, data)
            }
	    if (len != 0) 
	       fin(state, {code:TTEMISC+2, 
			wtf: 'Iterinit expected 0-size list from tyrant'})
	    state.cb(null, 'start')
	    state.miscCmd = 'iternext'
	    state.stream.write(
               bin.format('bbIIIS', TTMAGICNUM, state.cmd, 
			  state.miscCmd.length, /* opts */0, /* argCount */0, 
			  state.miscCmd),
               'binary'
            )
	 } else if (state.miscCmd == 'iternext') {
            if (err == TTEINVALID) {
               return fin(state, null, 'end')
            }
            if (err != TTESUCCESS) {
               return fin(state, {code:err}, data)
            }
	    if (len == 2) {
	       var ed = bin.read('I', data), data = ed[0], key = data.substr(0, ed[1][0])
	       data = data.slice(ed[1][0])
	       var ed = bin.read('I', data), data = ed[0], val = data.substr(0, ed[1][0])
	       var kj = eval(key), vj = eval(val)
	       state.cb(null, 'k-v', kj, vj)
	       if (kj == state.endKey) {
		  return fin(state, null, 'end')
	       }
	       state.stream.write(
		  bin.format('bbIIIS', TTMAGICNUM, state.cmd, 
			     state.miscCmd.length, /* opts */0, /* argCount */0, 
			     state.miscCmd),
		  'binary'
               )
	    } else {
	       fin(state, {code:TTEMISC+1, 
			wtf: 'Unexpected list length in ('+state.miscCmd+')'})
	    }
	 } else {
	    fin(state, {code:TTEMISC+1, 
			wtf: 'Not implemented misc command ('+state.miscCmd+')'})
	 }
         break
      default:
         fin(state, {code:TTEMISC+1, 
		     wtf: 'Not implemented onData event ('+state.cmd+')'})
         break
      }
   }

   function onDisconnect(state) {

   }

   /* Public interface */

   function put (k, v, cb, mod) {
      walloc(function (err, state) {
         var jk = JSON.stringify(k), jv = JSON.stringify(v)
	 if (cb.sort) { /* String ? => modifier, not cb*/
	    mod = cb
	    cb = undefined
	 }
         state.cmd = ({
	    'keep': TTCMDPUTKEEP, 
	    'append': TTCMDPUTCAT
	 })[mod] || (cb ? TTCMDPUT : TTCMDPUTNR)
	 if (state.cmd == TTCMDPUTCAT)
	    jv = '+' + jv
         state.cb = cb
         state.stream.write(
            bin.format('bbIISS', TTMAGICNUM, state.cmd, jk.length, jv.length, jk, jv),
            'binary'
         )
      })
   }

   function get(k, cb) {
      walloc(function (err, state) {
         var jk = JSON.stringify(k)
         state.cmd = TTCMDGET
         state.cb = cb
         state.stream.write(
            bin.format('bbIS', TTMAGICNUM, state.cmd, jk.length, jk),
            'binary'
         )
      })
   }

   function del(k, cb) {
      walloc(function (err, state) {
         var jk = JSON.stringify(k)
         state.cmd = TTCMDOUT
         state.cb = cb
         state.stream.write(
            bin.format('bbIS', TTMAGICNUM, state.cmd, jk.length, jk),
            'binary'
         )
      })
   }

   function iter(ks, ke, cb) {
      walloc(function (err, state) {
         var jks = JSON.stringify(ks)
         state.cmd = TTCMDMISC
	 state.miscCmd = 'iterinit'
	 state.endKey = ke
         state.cb = cb
	 if (jks) {
            state.stream.write(
               bin.format('bbIIISIS', TTMAGICNUM, state.cmd, 
			  state.miscCmd.length, /* opts */0, /* argCount */1,
			  state.miscCmd, jks.length, jks),
               'binary'
            )
	 } else {
	    state.stream.write(
               bin.format('bbIIIS',   TTMAGICNUM, state.cmd, 
			  state.miscCmd.length, /* opts */0, /* argCount */0, 
			  state.miscCmd),
               'binary'
            )
	 }
      })
   }

   function halt() {
      halted = true
      for (w in workers) {
	 workers[w].stream.destroy()
      }
   }

   /* State management */

   function fin(state) {
      var i, args=[]
      for (i=1; i < arguments.length; i++)
         args.push(arguments[i])
      state.cb && state.cb.apply(null, args)
      delete state.cb
      wfree(state.id)
   }

   /* The Maker */
   wrkCount = wrkCount || 8
   while(wrkCount--) {
      (function () {
         var conn = net.createConnection(port || TTDEFPORT, host), curr = {stream: conn}
         curr.id = wrkCount
         conn.addListener('connect', function () { onConnect(curr) })
         conn.addListener('drain', function () { onDrain(curr) })
         conn.addListener('data', function (data) { onData(curr,data) })
         conn.addListener('end', function () { onDisconnect(curr) })
         workers[wrkCount]=curr
         busy.push(wrkCount)
      })()
   }
   return {put: put, get: get, del: del, iter:iter, halt: halt}
}