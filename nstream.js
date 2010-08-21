/**
 * stream.js - my stream implementation.
 *
 * 2010, Michel Beloshitsky
 *
 * Licensed under the terms of MIT license. See COPYING file in the
 * root of distribution.
 */

var net = require('net')

exports.make = function (port, host, enc) {

   var netStr = net.createConnection(port || TTDEFPORT, host)

   var closed = false, encoding = enc || 'utf8'

   var drainCallback = null, readCallback = null, buffer = '', requestedSize = 0

   function checkReceivedEnough () {
      if (requestedSize && buffer.length >= requestedSize) {
         var resStr = buffer.substr(0, requestedSize)
         buffer = buffer.slice(requestedSize)
         requestedSize = 0
         readCallback(null, resStr)
      }
   }

   function write(str, cb) {
      drainCallback = cb
      netStr.write(str, encoding)
   }

   function read(size, cb) {
      readCallback = cb
      requestedSize = size
      checkReceivedEnough()
   }

   function close() {
      closed = true
      netStr.destroy()
   }

   /* Event handlers */

   netStr.addListener('connect' , function () {
      netStr.setEncoding(encoding)
   })

   netStr.addListener('end' , function () {

   })

   netStr.addListener('data' , function (data) {
      buffer += data
      checkReceivedEnough()
   })

   netStr.addListener('drain' , function () {
      if (drainCallback) {
         drainCallback()
         drainCallback = null
      }
   })

   function extListener(event, fun) {
      netStr.addListener(event, fun)
   }

   /* Public interface */

   return {read:read, write:write, close:close, on: extListener}
}