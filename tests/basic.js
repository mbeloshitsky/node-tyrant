/**
 * basic.js - basic tokyo tyrant to node.js connector test.
 *
 * 2010, Michel Beloshitsky
 *
 * Licensed under the terms of MIT license. See COPYING file in the
 * root of distribution.
 */

var tt = require('../tyrant').make()

function assert (assumption) {
   console.log(assumption ? 'Success' : 'Failure')
}

var tests = []

function nextTest() {
   var n = tests.shift()
   console.log('')
   n && n()
}

/*** Tests ***/

tests.push(function () {
   var test = 'Franky goes to hollywood.'

   console.log('** Basic put/get. **')

   tt.put('simple', test, function (err) {
      if (err) throw err
      tt.get('simple', function (err, data) {
	 console.log(test + ' = ' + data)
	 assert(test == data)
	 nextTest()
      })
   })
})

tests.push(function () {
   var test = ''

   console.log('** Huge value **')

   /* 0.9Mb value */
   for (var i = 0; i < 90000; i++) 
      test += 'aaaaaaaaaa'

   tt.put('hugeValue', test, function (err) {
      if (err) throw err
      tt.get('hugeValue', function (err, data) {
	 console.log(data.length + ' = ' + test.length)
	 assert(test == data)
	 nextTest()
      })
   })
})

tests.push(function () {
   var test1 = 'aaa', test2 = 'bbb'

   console.log('** Put keep test **')

   tt.put('putkeep', test1, function (err) {
      if (err) throw err
      tt.put('putkeep', test2, function (err) {
	 tt.get('putkeep', function (err, data) {
	    console.log(test1 + ' = ' + data)
	    assert(test1 == data)
	    nextTest()
	 })
      }, 'keep')
   })
})

tests.push(function () {
   var test1 = 'aaa', test2 = 'bbb'

   console.log('** Put append test **')

   tt.put('putappend', test1, function (err) {
      if (err) throw err
      tt.put('putappend', test2, function (err) {
	 if (err) throw err
	 tt.get('putappend', function (err, data) {
	    console.log(test1 + ' + ' + test2 + ' = ' + data)
	    assert((test1 + test2) == data)
	    nextTest()
	 })
      }, 'append')
   })
})

tests.push(function () {

   var rcvdkeys = []

   console.log('** Iter test **')

   tt.iter('putappend', 'putkeep', function (err, cmd, k, v) {
      if (err) throw err
      console.log(cmd)
      if (cmd == 'k-v') {
	 rcvdkeys.push(k)
	 console.log(k, v)
      }
      if (cmd == 'end') {
	 assert(rcvdkeys[0] == 'putappend' && rcvdkeys.pop() == 'putkeep')
	 nextTest()
      }
   })
})

tests.push(function () {
   console.log('** Halt test. Must now exit. **')
   tt.halt()
})

/*** Run tests ***/

nextTest()