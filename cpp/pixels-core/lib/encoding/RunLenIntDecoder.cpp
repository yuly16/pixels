//
// Created by liyu on 3/20/23.
//

#include "encoding/RunLenIntDecoder.h"

RunLenIntDecoder::RunLenIntDecoder(const std::shared_ptr <ByteBuffer>& bb, bool isSigned) {
    literals = new long[Constants::MAX_SCOPE];
    inputStream = bb;
    this->isSigned = isSigned;
    numLiterals = 0;
    used = 0;
	isRepeating = false;
}

void RunLenIntDecoder::close() {

}

RunLenIntDecoder::~RunLenIntDecoder() {
	if(literals != nullptr) {
		delete[] literals;
		literals = nullptr;
	}
}

long RunLenIntDecoder::next() {
    long result;
    if(used == numLiterals) {
        numLiterals = 0;
        used = 0;
        readValues();
    }
    result = literals[used++];
    return result;
}

void RunLenIntDecoder::readValues() {
	// read the first 2 bits and determine the encoding type
	isRepeating = false;
    int firstByte = (int) inputStream->get();
    if(firstByte < 0) {
        // TODO: logger.error
        used = 0;
        numLiterals = 0;
        return;
    }
    // here we do unsigned shift
    auto currentEncoding = (EncodingType) ((firstByte >> 6) & 0x03);
    switch (currentEncoding) {
        case RunLenIntEncoder::SHORT_REPEAT:
	    	readShortRepeatValues(firstByte);
	        break;
        case RunLenIntEncoder::DIRECT:
            readDirectValues(firstByte);
            break;
        case RunLenIntEncoder::PATCHED_BASE:
            readPatchedBaseValues(firstByte);
            break;
        case RunLenIntEncoder::DELTA:
		    readDeltaValues(firstByte);
		    break;
        default:
            throw InvalidArgumentException("Not supported encoding type.");
    }
}

void RunLenIntDecoder::readDirectValues(int firstByte) {
    // extract the number of fixed bits;
    uint8_t fbo = (firstByte >> 1) & 0x1f;
    int fb = encodingUtils.decodeBitWidth(fbo);

    // extract run length
    int len = (firstByte & 0x01) << 8;
    len |= inputStream->get();
    // runs are one off
    len += 1;

    // write the unpacked value and zigzag decode to result buffer
    readInts(literals, numLiterals, len, fb, inputStream);
    if(isSigned) {
        for (int i = 0; i < len; i++) {
            literals[numLiterals] = zigzagDecode(literals[numLiterals]);
            numLiterals++;
        }
    } else {
        numLiterals += len;
    }
}

long RunLenIntDecoder::zigzagDecode(long val) {
    return (long) (((uint64_t)val >> 1) ^ -(val & 1));
}

/**
 * Read bitpacked integers from input stream
 */
void RunLenIntDecoder::readInts(long *buffer, int offset, int len, int bitSize,
                           const std::shared_ptr<ByteBuffer> &input) {
    int bitsLeft = 0;
    int current = 0;
    switch (bitSize) {
	    case 1:
            encodingUtils.unrolledUnPack1(buffer, offset, len, input);
            return;
	    case 2:
			encodingUtils.unrolledUnPack2(buffer, offset, len, input);
		    return;
        case 4:
            encodingUtils.unrolledUnPack4(buffer, offset, len, input);
            return;
        case 8:
            encodingUtils.unrolledUnPack8(buffer, offset, len, input);
            return;
	    case 16:
		    encodingUtils.unrolledUnPack16(buffer, offset, len, input);
		    return;
	    case 24:
		    encodingUtils.unrolledUnPack24(buffer, offset, len, input);
		    return;
	    case 32:
		    encodingUtils.unrolledUnPack32(buffer, offset, len, input);
		    return;
	    case 40:
		    encodingUtils.unrolledUnPack40(buffer, offset, len, input);
		    return;
	    case 48:
		    encodingUtils.unrolledUnPack48(buffer, offset, len, input);
		    return;
	    case 56:
		    encodingUtils.unrolledUnPack56(buffer, offset, len, input);
		    return;
	    case 64:
		    encodingUtils.unrolledUnPack64(buffer, offset, len, input);
		    return;
        default:
            throw InvalidArgumentException("RunLenIntDecoder::readInts: "
                                           "not supported bitSize.");
    }
    // TODO: if notthe case, we should write the following code.
}

void RunLenIntDecoder::readDeltaValues(int firstByte) {
	// extract the number of fixed bits;
	uint8_t fb = (((uint32_t)firstByte) >> 1) & 0x1f;
	if(fb != 0) {
		fb = encodingUtils.decodeBitWidth(fb);
	}


	// extract the blob run length
	int len = (firstByte & 0x01) << 8;
	len |= inputStream->get();

	// read the first value stored as vint
	long firstVal = 0;
	if (isSigned) {
		firstVal = readVslong(inputStream);
	}
	else {
		firstVal = readVulong(inputStream);
	}

	// store first value to result buffer
	long prevVal = firstVal;
	literals[numLiterals++] = firstVal;

	// if fixed bits is 0 then all values have fixed delta
	if (fb == 0) {
		// read the fixed delta value stored as vint (deltas
		// can be negative even if all number are positive)
		long fd = readVslong(inputStream);
		if (fd == 0) {
			isRepeating = true;
			assert(numLiterals == 1);
			for(int i = 0; i < len; i++) {
				literals[numLiterals + i] = literals[0];
			}
			numLiterals += len;
		}
		else {
			// add fixed deltas to adjacent values
			for (int i = 0; i < len; i++) {
				literals[numLiterals] = literals[numLiterals - 1] + fd;
				numLiterals++;
			}
		}
	}
	else {
		long deltaBase = readVslong(inputStream);
		// add delta base and first value
		literals[numLiterals++] = firstVal + deltaBase;
		prevVal = literals[numLiterals - 1];
		len -= 1;

		// write the unpacked values, add it to previous values and store final
		// value to result buffer. if the delta base value is negative then it
		// is a decreasing sequence else an increasing sequence
		readInts(literals, numLiterals, len, fb, inputStream);
		while (len > 0) {
			if (deltaBase < 0) {
				literals[numLiterals] = prevVal - literals[numLiterals];
			}
			else {
				literals[numLiterals] = prevVal + literals[numLiterals];
			}
			prevVal = literals[numLiterals];
			len--;
			numLiterals++;
		}
	}

}

/**
     * Read an unsigned long from the input stream, using little endian.
     * @param in the input stream.
     * @return the long value.
     * @throws IOException if an I/O error occurs.
 */
long RunLenIntDecoder::readVulong(const std::shared_ptr<ByteBuffer> &input) {
	long result = 0;
	long b;
	int offset = 0;
	do {
		b = input->get();
		if(b == -1) {
			throw InvalidArgumentException("Reading Vulong past EOF");
		}
		result |= (0x7f & b) << offset;
		offset += 7;
	} while(b >= 0x80);
	return result;
}

/**
     * Read a signed long from the input stream, using little endian.
     * @param in the input stream.
     * @return the long value.
     * @throws IOException if an I/O error occurs.
 */
long RunLenIntDecoder::readVslong(const std::shared_ptr<ByteBuffer> &input) {
	long result = readVulong(input);
	return (((uint64_t)result) >> 1) ^ -(result & 1);
}

void RunLenIntDecoder::readShortRepeatValues(int firstByte) {
	// read the number of bytes occupied by the value
	int size = (((uint32_t)firstByte) >> 3) & 0x07;
	// number of bytes are one off
	size += 1;

	// read the run length
	int len = firstByte & 0x07;
	// run length values are stored only after MIN_REPEAT value is met
	len += Constants::MIN_REPEAT;

	// read the repeated value which is stored using fixed bytes
	long val = bytesToLongBE(inputStream, size);

	if (isSigned) {
		val = zigzagDecode(val);
	}

	if (numLiterals != 0) {
		// currently this always holds, which makes peekNextAvailLength simpler.
		// if this changes, peekNextAvailLength should be adjusted accordingly.
		throw InvalidArgumentException("numLiterals is not zero");
	}

	// repeat the value for length times
	isRepeating = true;
	// TODO: this is not so useful and V1 reader doesn't do that. Fix? Same if delta == 0
	for (int i = 0; i < len; i++) {
		literals[i] = val;
	}
	numLiterals = len;
}

/**
     * Read n bytes from the input stream, and parse them into long value using big endian.
     * @param input the input stream.
     * @param n n bytes.
     * @return the long value.
     * @throws IOException if an I/O error occurs.
 */
long RunLenIntDecoder::bytesToLongBE(const std::shared_ptr<ByteBuffer> &input, int n) {
	long out = 0;
	long val = 0;
	while (n > 0) {
		n--;
		// store it in a long and then shift else integer overflow will occur
		val = input->get();
		out |= (val << (n * 8));
	}
	return out;
}
bool RunLenIntDecoder::hasNext() {
	return used != numLiterals || (inputStream->size() - inputStream->getReadPos()) > 0;
}

void RunLenIntDecoder::readPatchedBaseValues(int firstByte) {
    // extract the number of fixed bits
    int fbo = (((uint32_t)firstByte) >> 1) & 0x1f;
    int fb = encodingUtils.decodeBitWidth(fbo);

    // extract the run length of data blob
    int len = (firstByte & 0x01) << 8;
    len |= inputStream->get();
    // runs are always one off
    len += 1;

    // extract the number of bytes occupied by base
    int thirdByte = inputStream->get();
    int bw = (((uint32_t)thirdByte) >> 5) & 0x07;
    // base width is one off
    bw += 1;

    // extract patch width
    int pwo = thirdByte & 0x1f;
    int pw = encodingUtils.decodeBitWidth(pwo);

    // read fourth byte and extract patch gap width
    int fourthByte = inputStream->get();
    int pgw = (((uint32_t)fourthByte) >> 5) & 0x07;
    // patch gao width is one off
    pgw += 1;

    // extract the length of the patch list
    int pl = fourthByte & 0x1f;

    // read the next base width number of bytes to extract base value
    long base = bytesToLongBE(inputStream, bw);
    long mask = (1L << ((bw * 8) - 1));
    // if MSB of base value is 1 then base is negative value else positive
    if ((base & mask) != 0) {
        base = base & ~mask;
        base = -base;
    }

    // unpack the data blob
    long * unpacked = new long[len];
    readInts(unpacked, 0, len, fb, inputStream);

    // unpack the patch blob
    long * unpackedPatch = new long[pl];

    if ((pw + pgw) > 64) {
        throw InvalidArgumentException("pw add pgw is bigger than 64");
        return;
    }
    int bitSize = encodingUtils.getClosestFixedBits(pw + pgw);
    readInts(unpackedPatch, 0, pl, bitSize, inputStream);

    // apply the patch directly when adding the packed data
    int patchIdx = 0;
    long currGap = 0;
    long currPatch = 0;
    long patchMask = ((1L << pw) - 1);

    currGap = (((uint64_t)unpackedPatch[patchIdx]) >> pw);
    currPatch = unpackedPatch[patchIdx] & patchMask;
    long actualGap = 0;

    // special case: gap is greater than 255 then patch value will be 0
    // if gap is smaller or equal than 255 then patch value cannot be 0
    while (currGap == 255 && currPatch == 0)
    {
        actualGap += 255;
        patchIdx++;
        currGap = (((uint64_t)unpackedPatch[patchIdx]) >> pw);
        currPatch = unpackedPatch[patchIdx] & patchMask;
    }
    // and the left over gap
    actualGap += currGap;

    // unpack data blob, patch it (if required), add base to get final result
    for (int i = 0; i < len; i++)
    {
        if (i == actualGap) {
            // extract the patch value
            long patchedVal = unpacked[i] | (currPatch << fb);
            // add base to patched value
            literals[numLiterals++] = base + patchedVal;
            // increment the patch to point to next entry in patch list
            patchIdx++;
            if (patchIdx < pl) {
                // read the next gap and patch
                currGap = (((uint64_t)unpackedPatch[patchIdx]) >> pw);
                currPatch = unpackedPatch[patchIdx] & patchMask;
                actualGap = 0;

                // special case: gap is grater than 255 then patch will be 0
                // if gap is smaller or equal than 255 then patch cannot be 0
                while (currGap == 255 && currPatch == 0)
                {
                    actualGap += 255;
                    patchIdx++;
                    currGap = (((uint64_t)unpackedPatch[patchIdx]) >> pw);
                    currPatch = unpackedPatch[patchIdx] & patchMask;
                }
                // add the left over gap
                actualGap += currGap;

                // next gap is relative to the current gap
                actualGap += i;
            }
        }
        else {
            // no patching required. add base to unpacked value to get final value
            literals[numLiterals++] = base + unpacked[i];
        }
    }
    delete[] unpackedPatch;
    delete[] unpacked;
}
