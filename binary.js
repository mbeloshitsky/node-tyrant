/**
 * binary.js - binary data helper routines.
 *
 * 2010, Michel Beloshitsky
 *
 * Licensed under the terms of MIT license. See COPYING file in the
 * root of distribution.
 */

/**
 * Basic idea of this format functions borrowed from "binary format" 
 * routine of tcl standart library. But abbreviations now did not match.
 *
 * This implementation of binary routines is not full; it's tuned for 
 * tokyo tyrant connector.
 *
 * Abbreviations:
 *
 * b - byte (octet).
 * I - 32-bit big-endian integer
 * S - string (sequence of chars) Only avaible in "format" function.
 *
 * Example:
 *
 * > require('./binary').format('bbIIS', 1, 2, 4, 5, 'test') 
 *
 * produces:
 *
 * > 0x01 0x02 0x00 0x00  0x00 0x04 0x00 0x00  
 * > 0x00 0x05 0x74 0x65  0x73 0x74
 *
 * > require('./binary').read(formatString, binaryStringToParse)
 *
 * acts vice versa.
 */

exports.format = function () {
   var out = '', args = arguments, format = args[0].split(''), argi = 1
   format.map(function (spec) {
      switch (spec) {
      case 'b': out += String.fromCharCode(args[argi])
         break
      case 'I':
         var iv = args[argi], i=4, ov = []
         while (i--) {
            ov.push(String.fromCharCode(iv & 0xFF))
            iv >>= 8
         }
         out+=ov.reverse().join('')
         break
      case 'S': out += args[argi]
         break
      default:
         throw 'Binary format syntax error'
      }
      argi++
   })
   return out
}

exports.read = function (fmtStr, str) {
   var out = [], i = 0
   fmtStr.split('').map(function (spec) {
      switch (spec) {
      case 'b': 
	 out.push(str.charCodeAt(i)); i++
         break
      case 'I':
         var j = -1, v = 0
	 while (++j < 4) {
	    v += (str.charCodeAt(i + j) & 0xFF) << (8 * (3 - j))
	 }
	 out.push(v); i += 4
         break
      default:
         throw 'Binary read syntax error'
      }
   })
   return [str.slice(i), out]
}