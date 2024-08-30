package io.velo.command;

public class TestLZFDecompress {
    /*
 size_t lzf_decompress(const void *const in_data, size_t in_len, void *out_data, size_t out_len) {
  u8 const *ip = (const u8 *)in_data;
  u8 *op = (u8 *)out_data;
  u8 const *const in_end = ip + in_len;
  u8 *const out_end = op + out_len;

  while (ip < in_end) {
    unsigned int ctrl;
    ctrl = *ip++;

    if (ctrl < (1 << 5))
    {
        ctrl++;

        if (op + ctrl > out_end) {
            SET_ERRNO(E2BIG);
            return 0;
        }

#if CHECK_INPUT
        if (ip + ctrl > in_end) {
            SET_ERRNO(EINVAL);
            return 0;
        }
#endif
#ifdef lzf_movsb
        lzf_movsb(op, ip, ctrl);
#else
        switch (ctrl) {
            case 32:
          *op++ = *ip++;
            case 31:
          *op++ = *ip++;
            case 30:
          *op++ = *ip++;
            case 29:
          *op++ = *ip++;
            case 28:
          *op++ = *ip++;
            case 27:
          *op++ = *ip++;
            case 26:
          *op++ = *ip++;
            case 25:
          *op++ = *ip++;
            case 24:
          *op++ = *ip++;
            case 23:
          *op++ = *ip++;
            case 22:
          *op++ = *ip++;
            case 21:
          *op++ = *ip++;
            case 20:
          *op++ = *ip++;
            case 19:
          *op++ = *ip++;
            case 18:
          *op++ = *ip++;
            case 17:
          *op++ = *ip++;
            case 16:
          *op++ = *ip++;
            case 15:
          *op++ = *ip++;
            case 14:
          *op++ = *ip++;
            case 13:
          *op++ = *ip++;
            case 12:
          *op++ = *ip++;
            case 11:
          *op++ = *ip++;
            case 10:
          *op++ = *ip++;
            case 9:
          *op++ = *ip++;
            case 8:
          *op++ = *ip++;
            case 7:
          *op++ = *ip++;
            case 6:
          *op++ = *ip++;
            case 5:
          *op++ = *ip++;
            case 4:
          *op++ = *ip++;
            case 3:
          *op++ = *ip++;
            case 2:
          *op++ = *ip++;
            case 1:
          *op++ = *ip++;
        }
#endif
    } else
    {
        unsigned int len = ctrl >> 5;

        u8 *ref = op - ((ctrl & 0x1f) << 8) - 1;

#if CHECK_INPUT
        if (ip >= in_end) {
            SET_ERRNO(EINVAL);
            return 0;
        }
#endif
        if (len == 7) {
            len += *ip++;
#if CHECK_INPUT
            if (ip >= in_end) {
                SET_ERRNO(EINVAL);
                return 0;
            }
#endif
        }

        ref -= *ip++;

        if (op + len + 2 > out_end) {
            SET_ERRNO(E2BIG);
            return 0;
        }

        if (ref < (u8 *)out_data) {
        SET_ERRNO(EINVAL);
        return 0;
    }

#ifdef lzf_movsb
        len += 2;
        lzf_movsb(op, ref, len);
#else
        switch (len) {
            default:
                len += 2;

                if (op >= ref + len) {
                    memcpy(op, ref, len);
                    op += len;
                } else {
                    do *op++ = *ref++;
                    while (--len);
                }

                break;

            case 9:
          *op++ = *ref++;
            case 8:
          *op++ = *ref++;
            case 7:
          *op++ = *ref++;
            case 6:
          *op++ = *ref++;
            case 5:
          *op++ = *ref++;
            case 4:
          *op++ = *ref++;
            case 3:
          *op++ = *ref++;
            case 2:
          *op++ = *ref++;
            case 1:
          *op++ = *ref++;
            case 0:
          *op++ = *ref++;
          *op++ = *ref++;
        }
#endif
    }
}

  return op - (u8 *)out_data;
          }
     */

    void lzfDecompress(byte[] inData, int inPosition, int inLen, byte[] outData, int outLen) {
        int ip = inPosition;
        int op = 0;
        int inEnd = ip + inLen;
        int outEnd = op + outLen;

        while (ip < inEnd) {
            int ctrl = inData[ip++] & 0xFF;

            if (ctrl < (1 << 5)) {
                ctrl++;

                if (op + ctrl > outEnd) {
                    throw new IllegalArgumentException("E2BIG");
                }

                if (ip + ctrl > inEnd) {
                    throw new IllegalArgumentException("EINVAL");
                }

                switch (ctrl) {
                    case 32:
                        outData[op++] = inData[ip++];
                    case 31:
                        outData[op++] = inData[ip++];
                    case 30:
                        outData[op++] = inData[ip++];
                    case 29:
                        outData[op++] = inData[ip++];
                    case 28:
                        outData[op++] = inData[ip++];
                    case 27:
                        outData[op++] = inData[ip++];
                    case 26:
                        outData[op++] = inData[ip++];
                    case 25:
                        outData[op++] = inData[ip++];
                    case 24:
                        outData[op++] = inData[ip++];
                    case 23:
                        outData[op++] = inData[ip++];
                    case 22:
                        outData[op++] = inData[ip++];
                    case 21:
                        outData[op++] = inData[ip++];
                    case 20:
                        outData[op++] = inData[ip++];
                    case 19:
                        outData[op++] = inData[ip++];
                    case 18:
                        outData[op++] = inData[ip++];
                    case 17:
                        outData[op++] = inData[ip++];
                    case 16:
                        outData[op++] = inData[ip++];
                    case 15:
                        outData[op++] = inData[ip++];
                    case 14:
                        outData[op++] = inData[ip++];
                    case 13:
                        outData[op++] = inData[ip++];
                    case 12:
                        outData[op++] = inData[ip++];
                    case 11:
                        outData[op++] = inData[ip++];
                    case 10:
                        outData[op++] = inData[ip++];
                    case 9:
                        outData[op++] = inData[ip++];
                    case 8:
                        outData[op++] = inData[ip++];
                    case 7:
                        outData[op++] = inData[ip++];
                    case 6:
                        outData[op++] = inData[ip++];
                    case 5:
                        outData[op++] = inData[ip++];
                    case 4:
                        outData[op++] = inData[ip++];
                    case 3:
                        outData[op++] = inData[ip++];
                    case 2:
                        outData[op++] = inData[ip++];
                    case 1:
                        outData[op++] = inData[ip++];
                }
            } else {
                int len = (ctrl >> 5) & 0xFF;
                int refIndex = op - ((ctrl & 0x1f) << 8) - 1;

                if (ip >= inEnd) {
                    throw new IllegalArgumentException("EINVAL");
                }

                if (len == 7) {
                    len += inData[ip++];
                    if (ip >= inEnd) {
                        throw new IllegalArgumentException("EINVAL");
                    }
                }

                refIndex -= inData[ip++];

                if (op + len + 2 > outEnd) {
                    throw new IllegalArgumentException("E2BIG");
                }

                if (refIndex < 0) {
                    throw new IllegalArgumentException("EINVAL");
                }

                switch (len) {
                    default:
                        len += 2;

                        if (op >= refIndex + len) {
                            System.arraycopy(outData, refIndex, outData, op, len);
                            op += len;
                        } else {
                            do {
                                outData[op++] = outData[refIndex++];
                            } while (--len > 0);
                        }

                        break;

                    case 9:
                        outData[op++] = outData[refIndex++];
                    case 8:
                        outData[op++] = outData[refIndex++];
                    case 7:
                        outData[op++] = outData[refIndex++];
                    case 6:
                        outData[op++] = outData[refIndex++];
                    case 5:
                        outData[op++] = outData[refIndex++];
                    case 4:
                        outData[op++] = outData[refIndex++];
                    case 3:
                        outData[op++] = outData[refIndex++];
                    case 2:
                        outData[op++] = outData[refIndex++];
                    case 1:
                        outData[op++] = outData[refIndex++];
                    case 0:
                        outData[op++] = outData[refIndex++];
                        outData[op++] = outData[refIndex++];
                }
            }
        }
    }

    public static void main(String[] args) {
        byte[] compressedBytes = new byte[]{0, 1, 97, 97, -32, 79, 0, 0, 98, -32, 80, 0, 0, 99, -32, 78, 0, 1, 99, 99};
        byte[] rawBytes = new byte[270];
        new TestLZFDecompress().lzfDecompress(compressedBytes, 1, compressedBytes.length - 1, rawBytes, rawBytes.length);
        System.out.println(new String(rawBytes));
    }

}
