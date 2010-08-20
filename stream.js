/**
 * stream.js - my stream implementation.
 *
 * 2010, Michel Beloshitsky
 *
 * Licensed under the terms of MIT license. See COPYING file in the
 * root of distribution.
 */
exports.make = function (netStr, enc) {

   var closed = false, encoding = enc || 'utf8'

   var drainCallback = null, readCallback = null, buffer = '', requestedSize = 0

   function checkReceivedEnough () {
      if (readCallback && buffer.length >= requestedSize) {
         readCallback(buffer.substr(0, requestedSize))
         readCallback = null
         buffer = buffer.slice(requestedSize)
         requestedSize = 0
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

   /* Public interface */

   return {read:read, write:write, close: close}
}