/**
 * stream.js - my stream implementation.
 */
exports.make = function (netStr) {

   var drainCallback = null, readCallback = null, buffer = '', requestedSize = 0

   function write(str, cb) {
      
   }

   function read(size, cb) {
      
   }

   return {read:read, write:write}
}