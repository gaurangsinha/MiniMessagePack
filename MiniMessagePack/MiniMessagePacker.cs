/* MiniMessagePack - Simple MessagePack(http://msgpack.org/) Parser for C#
 * See https://github.com/shogo82148/MiniMessagePack to get more information.
 * 
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Ichinose Shogo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
*/

using System;
using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Collections;

namespace MiniMessagePack {

	/// <summary>
	/// Message pack.
	/// <seealso cref="https://github.com/msgpack/msgpack/blob/master/spec.md"/>
	/// </summary>
	class MsgPack {

		// POSITIVE_FIXINT = 0x00 to 0x7f
		public const byte POSITIVE_FIX_INT = 0x00;
		public const byte POSITIVE_FIX_INT_MAX = 0x7f;
		public const byte POSITIVE_FIX_INT_COUNT = POSITIVE_FIX_INT_MAX - POSITIVE_FIX_INT;

		// FIX_MAP = 0x80 to 0x8f 
		public const byte FIX_MAP = 0x80;
		public const byte FIX_MAP_MAX = 0x8f;
		public const byte FIX_MAP_COUNT = FIX_MAP_MAX - FIX_MAP + 1;

		// FIX_ARRAY = 0x90 to 0x9f
		public const byte FIX_ARRAY = 0x90;
		public const byte FIX_ARRAY_MAX = 0x9f;
		public const byte FIX_ARRAY_COUNT = FIX_ARRAY_MAX - FIX_ARRAY + 1;

		// FIX_STR = 0xa0 to 0xbf
		public const byte FIX_STR = 0xa0;
		public const byte FIX_STR_MAX = 0xbf;
		public const byte FIX_STR_COUNT = FIX_STR_MAX - FIX_STR + 1;

		public const byte NIL = 0xc0;
		public const byte _NEVER_USED_ = 0xc1;
		public const byte FALSE = 0xc2;
		public const byte TRUE = 0xc3;
		public const byte BIN_8 = 0xc4;
		public const byte BIN_16 = 0xc5;
		public const byte BIN_32 = 0xc6;
		public const byte EXT_8 = 0xc7;
		public const byte EXT_16 = 0xc8;
		public const byte EXT_32 = 0xc9;
		public const byte FLOAT_32 = 0xca;
		public const byte FLOAT_64 = 0xcb;
		public const byte UINT_8 = 0xcc;
		public const byte UINT_16 = 0xcd;
		public const byte UINT_32 = 0xce;
		public const byte UINT_64 = 0xcf;
		public const byte INT_8 = 0xd0;
		public const byte INT_16 = 0xd1;
		public const byte INT_32 = 0xd2;
		public const byte INT_64 = 0xd3;
		public const byte FIXEXT_1 = 0xd4;
		public const byte FIXEXT_2 = 0xd5;
		public const byte FIXEXT_4 = 0xd6;
		public const byte FIXEXT_8 = 0xd7;
		public const byte FIXEXT_16 = 0xd8;
		public const byte STR_8 = 0xd9;
		public const byte STR_16 = 0xda;
		public const byte STR_32 = 0xdb;
		public const byte ARRAY_16 = 0xdc;
		public const byte ARRAY_32 = 0xdd;
		public const byte MAP_16 = 0xde;
		public const byte MAP_32 = 0xdf;

		// NEGATIVE_FIXINT = 0xe0 to 0xff;
		public const byte NEGATIVE_FIX_INT = 0xe0;
		public const byte NEGATIVE_FIX_INT_MAX = 0xff;
		public const byte NEGATIVE_FIX_INT_COUNT = NEGATIVE_FIX_INT_MAX - NEGATIVE_FIX_INT;

		public const int COUNT_1_NIBBLE = 16;
		public const int COUNT_1_BYTE = COUNT_1_NIBBLE << 4;
		public const ulong COUNT_2_BYTE = COUNT_1_BYTE << 8;
		public const ulong COUNT_4_BYTE = COUNT_2_BYTE << 16;
		public const ulong COUNT_8_BYTE = ulong.MaxValue;
	}

	/// <summary>
	/// Mini message packer.
	/// </summary>
	public class MiniMessagePacker {

		const int BITS_IN_BYTE = 8;
		const int MOVE_1_BYTE = BITS_IN_BYTE * 1;
		const int MOVE_2_BYTES = BITS_IN_BYTE * 2;
		const int MOVE_3_BYTES = BITS_IN_BYTE * 3;
		const int MOVE_4_BYTES = BITS_IN_BYTE * 4;
		const int MOVE_5_BYTES = BITS_IN_BYTE * 5;
		const int MOVE_6_BYTES = BITS_IN_BYTE * 6;
		const int MOVE_7_BYTES = BITS_IN_BYTE * 7;

		// This method is not efficient, but more succinct 
		private static void ReverseBytes(ref byte[] original, ref byte[] result, int numOfBytes) {
			for(int i=0; i<numOfBytes; i++) {
				result[numOfBytes-i-1] = original[i];
			}
		}

		const int TEMP_LENGTH = 8;
		const int DEFAULT_STRING_BUFFER_LENGTH = 128;

		private byte[] tmp0 = new byte[TEMP_LENGTH];
		private byte[] tmp1 = new byte[TEMP_LENGTH];
		private byte[] string_buf = new byte[DEFAULT_STRING_BUFFER_LENGTH];
		private Encoding encoder = Encoding.UTF8;

		public byte[] Pack (object o) {
			using (MemoryStream ms = new MemoryStream ()) {
				Pack (ms, o);
				return ms.ToArray ();
			}
		}

		public void Pack(Stream s, object o) {
			IDictionary asDict;
			string asStr;
			IList asList;
			if (o == null)
				PackNull (s);
			else if ((asStr = o as string) != null)
				Pack (s, asStr);
			else if ((asList = o as IList) != null)
				Pack (s, asList);
			else if ((asDict = o as IDictionary) != null)
				Pack (s, asDict);
			else if (o is bool)
				Pack (s, (bool)o);
			else if (o is sbyte)
				Pack (s, (sbyte)o);
			else if (o is byte)
				Pack (s, (byte)o);
			else if (o is short)
				Pack (s, (short)o);
			else if (o is ushort)
				Pack (s, (ushort)o);
			else if (o is int)
				Pack (s, (int)o);
			else if (o is uint)
				Pack (s, (uint)o);
			else if (o is long)
				Pack (s, (long)o);
			else if (o is ulong)
				Pack (s, (ulong)o);
			else if (o is float)
				Pack (s, (float)o);
			else if (o is double)
				Pack (s, (double)o);
			else
				Pack (s, o.ToString ());
		}

		private void PackNull(Stream s) {
			s.WriteByte (MsgPack.NIL);
		}

		private void Pack (Stream s, IList list) {
			int count = list.Count;
			if (count < (MsgPack.FIX_ARRAY_COUNT)) {
				s.WriteByte ((byte)(MsgPack.FIX_ARRAY + count));
			} else if (count < 0x10000) {
				s.WriteByte (MsgPack.ARRAY_16);
				Write (s, (ushort)count);
			} else {
				s.WriteByte (MsgPack.ARRAY_32);
				Write (s, (uint)count);
			}
			foreach (object o in list) {
				Pack (s, o);
			}
		}

		private void Pack (Stream s, IDictionary dict) {
			int count = dict.Count;
			if (count < 16) {
				s.WriteByte ((byte)(MsgPack.FIX_MAP + count));
			} else if (count < 0x10000) {
				s.WriteByte (MsgPack.MAP_16);
				Write (s, (ushort)count);
			} else {
				s.WriteByte (MsgPack.MAP_32);
				Write (s, (uint)count);
			}
			foreach (object key in dict.Keys) {
				Pack (s, key);
				Pack (s, dict [key]);
			}
		}

		private void Pack(Stream s, bool val) {
			s.WriteByte (val ? MsgPack.TRUE : MsgPack.FALSE);
		}

		private void Pack(Stream s, sbyte val) {
			unchecked {
				if (val >= -32) {
					s.WriteByte ((byte)val);
				} else {
					tmp0 [0] = MsgPack.INT_8;
					tmp0 [1] = (byte)val;
					s.Write (tmp0, 0, 2);
				}
			}
		}

		private void Pack(Stream s, byte val) {
			if (val <= MsgPack.POSITIVE_FIX_INT_MAX) {
				s.WriteByte (val);
			} else {
				tmp0 [0] = MsgPack.UINT_8;
				tmp0 [1] = val;
				s.Write (tmp0, 0, 2);
			}
		}

		private void Pack(Stream s, short val) {
			unchecked {
				if (val >= 0) {
					Pack (s, (ushort)val);
				} else if (val >= -128) {
					Pack (s, (sbyte)val);
				} else {
					s.WriteByte (MsgPack.INT_16);
					Write (s, (ushort)val);
				}
			}
		}

		private void Pack(Stream s, ushort val) {
			unchecked {
				if (val < 0x100) {
					Pack (s, (byte)val);
				} else {
					s.WriteByte (MsgPack.UINT_16);
					Write (s, (ushort)val);
				}
			}
		}

		private void Pack(Stream s, int val) {
			unchecked {
				if (val >= 0) {
					Pack (s, (uint)val);
				} else if (val >= -128) {
					Pack (s, (sbyte)val);
				} else if (val >= -0x8000) {
					s.WriteByte (MsgPack.INT_16);
					Write (s, (ushort)val);
				} else {
					s.WriteByte (MsgPack.INT_32);
					Write (s, (uint)val);
				}
			}
		}

		private void Pack(Stream s, uint val) {
			unchecked {
				if (val < 0x100) {
					Pack (s, (byte)val);
				} else if (val < 0x10000) {
					s.WriteByte (MsgPack.UINT_16);
					Write (s, (ushort)val);
				} else {
					s.WriteByte (MsgPack.UINT_32);
					Write (s, (uint)val);
				}
			}
		}

		private void Pack(Stream s, long val) {
			unchecked {
				if (val >= 0) {
					Pack (s, (ulong)val);
				} else if (val >= -128) {
					Pack (s, (sbyte)val);
				} else if (val >= -0x8000) {
					s.WriteByte (MsgPack.INT_16);
					Write (s, (ushort)val);
				} else if (val >= -0x80000000) {
					s.WriteByte (MsgPack.INT_32);
					Write (s, (uint)val);
				} else {
					s.WriteByte (MsgPack.INT_64);
					Write (s, (ulong)val);
				}
			}
		}

		private void Pack(Stream s, ulong val) {
			unchecked {
				if (val < 0x100) {
					Pack (s, (byte)val);
				} else if (val < 0x10000) {
					s.WriteByte (MsgPack.UINT_16);
					Write (s, (ushort)val);
				} else if (val < 0x100000000) {
					s.WriteByte (MsgPack.UINT_32);
					Write (s, (uint)val);
				} else {
					s.WriteByte (MsgPack.UINT_64);
					Write (s, val);
				}
			}
		}

		private void Pack(Stream s, float val) {
			var bytes = BitConverter.GetBytes (val);
			s.WriteByte (MsgPack.FLOAT_32);
			if (BitConverter.IsLittleEndian) {
				//ReverseBytes(ref bytes, ref tmp0, 4);
				tmp0 [0] = bytes [3];
				tmp0 [1] = bytes [2];
				tmp0 [2] = bytes [1];
				tmp0 [3] = bytes [0];
				s.Write (tmp0, 0, 4);
			} else {
				s.Write (bytes, 0, 4);
			}
		}

		private void Pack(Stream s, double val) {
			var bytes = BitConverter.GetBytes (val);
			s.WriteByte (MsgPack.FLOAT_64);
			if (BitConverter.IsLittleEndian) {
				//ReverseBytes(ref bytes, ref tmp0, 8);
				tmp0 [0] = bytes [7];
				tmp0 [1] = bytes [6];
				tmp0 [2] = bytes [5];
				tmp0 [3] = bytes [4];
				tmp0 [4] = bytes [3];
				tmp0 [5] = bytes [2];
				tmp0 [6] = bytes [1];
				tmp0 [7] = bytes [0];
				s.Write (tmp0, 0, 8);
			} else {
				s.Write (bytes, 0, 8);
			}
		}

		private void Pack(Stream s, string val) {
			var bytes = encoder.GetBytes (val);
			if (bytes.Length < 0x20) {
				s.WriteByte ((byte)(MsgPack.FIX_STR + bytes.Length));
			} else if (bytes.Length < 0x100) {
				s.WriteByte (MsgPack.STR_8);
				s.WriteByte ((byte)(bytes.Length));
			} else if (bytes.Length < 0x10000) {
				s.WriteByte (MsgPack.STR_16);
				Write (s, (ushort)(bytes.Length));
			} else {
				s.WriteByte (MsgPack.STR_32);
				Write (s, (uint)(bytes.Length));
			}
			s.Write (bytes, 0, bytes.Length);
		}

		private void Write(Stream s, ushort val) {
			unchecked {
				tmp0 [0] = (byte)(val >> MOVE_1_BYTE);
				tmp0 [1] = (byte)val;
				s.Write (tmp0, 0, 2);
			}
		}

		private void Write(Stream s, uint val) {
			unchecked {
				tmp0 [0] = (byte)(val >> MOVE_3_BYTES);
				tmp0 [1] = (byte)(val >> MOVE_2_BYTES);
				tmp0 [2] = (byte)(val >> MOVE_1_BYTE);
				tmp0 [3] = (byte)val;
				s.Write (tmp0, 0, 4);
			}
		}

		private void Write(Stream s, ulong val) {
			unchecked {
				tmp0 [0] = (byte)(val >> MOVE_7_BYTES);
				tmp0 [1] = (byte)(val >> MOVE_6_BYTES);
				tmp0 [2] = (byte)(val >> MOVE_5_BYTES);
				tmp0 [3] = (byte)(val >> MOVE_4_BYTES);
				tmp0 [4] = (byte)(val >> MOVE_3_BYTES);
				tmp0 [5] = (byte)(val >> MOVE_2_BYTES);
				tmp0 [6] = (byte)(val >> MOVE_1_BYTE);
				tmp0 [7] = (byte)val;
				s.Write (tmp0, 0, 8);
			}
		}

		public object Unpack (byte[] buf, int offset, int size) {
			using (MemoryStream ms = new MemoryStream (buf, offset, size)) {
				return Unpack (ms);
			}
		}

		public object Unpack(byte[] buf) {
			return Unpack (buf, 0, buf.Length);
		}

		public object Unpack(Stream s) {
			int b = s.ReadByte ();
			if (b < 0) {
				throw new FormatException ();
			} else if (b <= MsgPack.POSITIVE_FIX_INT_MAX) { // positive fixint
				return (long)b;
			} else if (b <= MsgPack.FIX_MAP_MAX) { // fixmap
				return UnpackMap (s, b & 0x0f);
			} else if (b <= MsgPack.FIX_ARRAY_MAX) { // fixarray
				return UnpackArray (s, b & 0x0f);
			} else if (b <= MsgPack.FIX_STR_MAX) { // fixstr
				return UnpackString (s, b & 0x1f);
			} else if( b >= MsgPack.NEGATIVE_FIX_INT) { // negative fixint
				return (long)unchecked((sbyte)b);
			}
			switch (b) {
			case MsgPack.NIL:
				return null;
			case MsgPack.FALSE:
				return false;
			case MsgPack.TRUE:
				return true;
			case MsgPack.UINT_8: // uint8
				return (long)s.ReadByte ();
			case MsgPack.UINT_16: // uint16
				return UnpackUint16 (s);
			case MsgPack.UINT_32: // uint32
				return UnpackUint32 (s);
			case MsgPack.UINT_64: // uint64
				if (s.Read (tmp0, 0, 8) != 8) { 
					throw new FormatException ();
				}
				return ((long)tmp0 [0] << MOVE_7_BYTES) 
					| ((long)tmp0 [1] << MOVE_6_BYTES) 
					| ((long)tmp0 [2] << MOVE_5_BYTES) 
					| ((long)tmp0 [3] << MOVE_4_BYTES)
					+ ((long)tmp0 [4] << MOVE_3_BYTES) 
					| ((long)tmp0 [5] << MOVE_2_BYTES) 
					| ((long)tmp0 [6] << MOVE_1_BYTE) 
					| (long)tmp0 [7];
			case MsgPack.INT_8: // int8
				return (long)unchecked((sbyte)s.ReadByte ());
			case MsgPack.INT_16: // int16
				if (s.Read (tmp0, 0, 2) != 2) { 
					throw new FormatException ();
				}
				return (((long)unchecked((sbyte)tmp0[0])) << MOVE_1_BYTE) 
					| (long)tmp0[1];
			case MsgPack.INT_32: // int32
				if (s.Read (tmp0, 0, 4) != 4) { 
					throw new FormatException ();
				}
				return ((long)unchecked((sbyte)tmp0[0]) << MOVE_3_BYTES) 
									| ((long)tmp0[1] << MOVE_2_BYTES) 
									| ((long)tmp0[2] << MOVE_1_BYTE) 
									| (long)tmp0[3];
			case MsgPack.INT_64: // int64
				if (s.Read (tmp0, 0, 8) != 8) { 
					throw new FormatException ();
				}
				return ((long)unchecked((sbyte)tmp0[0]) << MOVE_7_BYTES) 
						| ((long)tmp0 [1] << MOVE_6_BYTES) 
						| ((long)tmp0 [2] << MOVE_5_BYTES) 
						| ((long)tmp0 [3] << MOVE_4_BYTES)
						+ ((long)tmp0 [4] << MOVE_3_BYTES) 
						| ((long)tmp0 [5] << MOVE_2_BYTES) 
						| ((long)tmp0 [6] << MOVE_1_BYTE) 
						| (long)tmp0 [7];
			case MsgPack.FLOAT_32: // float32
				s.Read (tmp0, 0, 4);
				if (BitConverter.IsLittleEndian) {
					//ReverseBytes(ref tmp0, ref tmp1, 4);
					tmp1[0] = tmp0[3];
					tmp1[1] = tmp0[2];
					tmp1[2] = tmp0[1];
					tmp1[3] = tmp0[0];
					return (double)BitConverter.ToSingle (tmp1, 0);
				} else {
					return (double)BitConverter.ToSingle (tmp0, 0);
				}
			case MsgPack.FLOAT_64: // float64
				s.Read (tmp0, 0, 8);
				if (BitConverter.IsLittleEndian) {
					//ReverseBytes(ref tmp0, ref tmp1, 8);
					tmp1[0] = tmp0[7];
					tmp1[1] = tmp0[6];
					tmp1[2] = tmp0[5];
					tmp1[3] = tmp0[4];
					tmp1[4] = tmp0[3];
					tmp1[5] = tmp0[2];
					tmp1[6] = tmp0[1];
					tmp1[7] = tmp0[0];
					return BitConverter.ToDouble (tmp1, 0);
				} else {
					return BitConverter.ToDouble (tmp0, 0);
				}
			case MsgPack.STR_8: // str8
				return UnpackString (s, s.ReadByte ());
			case MsgPack.STR_16: // str16
				return UnpackString (s, UnpackUint16 (s));
			case MsgPack.STR_32: // str32
				return UnpackString (s, UnpackUint32 (s));

			case MsgPack.BIN_8: // bin8
				return UnpackBinary (s, s.ReadByte ());
			case MsgPack.BIN_16: // bin16
				return UnpackBinary (s, UnpackUint16 (s));
			case MsgPack.BIN_32: // bin32
				return UnpackBinary (s, UnpackUint32 (s));

			case MsgPack.ARRAY_16: // array16
				return UnpackArray (s, UnpackUint16 (s));
			case MsgPack.ARRAY_32: // array32
				return UnpackArray (s, UnpackUint32 (s));

			case MsgPack.MAP_16: // map16
				return UnpackMap (s, UnpackUint16 (s));
			case MsgPack.MAP_32: // map32
				return UnpackMap (s, UnpackUint32 (s));
			}
			return null;
		}

		private long UnpackUint16(Stream s) {
			if (s.Read (tmp0, 0, 2) != 2) { 
				throw new FormatException ();
			}
			return (long)((tmp0[0] << MOVE_1_BYTE) | tmp0[1]);
		}

		private long UnpackUint32(Stream s) {
			if (s.Read (tmp0, 0, 4) != 4) { 
				throw new FormatException ();
			}
			return ((long)tmp0[0] << MOVE_3_BYTES) 
				| ((long)tmp0[1] << MOVE_2_BYTES) 
				| ((long)tmp0[2] << MOVE_1_BYTE) 
				| (long)tmp0[3];
		}

		private string UnpackString(Stream s, long len) {
			if (string_buf.Length < len) {
				string_buf = new byte[len];
			}
			s.Read (string_buf, 0, (int)len);
			return encoder.GetString(string_buf, 0, (int)len);
		}

		private byte[] UnpackBinary(Stream s, long len) {
			byte[] buf = new byte[len];
			s.Read (buf, 0, (int)len);
			return buf;
		}

		private List<object> UnpackArray(Stream s, long len) {
			var list = new List<object> ((int)len);
			for (long i = 0; i < len; i++) {
				list.Add (Unpack (s));
			}
			return list;
		}

		private Dictionary<string, object> UnpackMap(Stream s, long len) {
			var dict = new Dictionary<string, object> ((int)len);
			for (long i = 0; i < len; i++) {
				string key = Unpack (s) as string;
				object value = Unpack (s);
				if (key != null) {
					dict.Add (key, value);
				}
			}
			return dict;
		}

	}
}

