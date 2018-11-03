package com.kaszub.parso.impl

/**
  * This is an class to store constants for parsing the sas7bdat file (byte offsets, column formats, accuracy) as well as
  * the standard constants of time and the sizes of standard data types.
  */
trait SasFileConstants {
    /**
      * The size of the long value type in bytes.
      */
    val BytesInLong = 8
    /**
      * The size of the double value type in bytes.
      */
    val BytesInDouble = 8
    /**
      * The size of the int value type in bytes.
      */
    val BytesInInt = 4
    /**
      * If a value with the length of {@link SasFileConstants#ALIGN_1_LENGTH} bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#ALIGN_1_OFFSET} bytes offset equals to ALIGN_1_CHECKER_VALUE, then starting from
      * the {@link SasFileConstants#DATE_CREATED_OFFSET} bytes offset every offset should increase by
      * {@link SasFileConstants#ALIGN_1_VALUE} bytes.
      */
    val Align1CheckerValue = 51
    /**
      * If a value with the length of {@link SasFileConstants#ALIGN_1_LENGTH} bytes stored in the sas7bdat file with
      * a ALIGN_1_OFFSET bytes offset equals to {@link SasFileConstants#ALIGN_1_CHECKER_VALUE}, then starting from
      * the {@link SasFileConstants#DATE_CREATED_OFFSET} bytes offset every offset should increase by
      * {@link SasFileConstants#ALIGN_1_VALUE} bytes.
      */
    val Align1Offset = 32L
    /**
      * If a value with the length of ALIGN_1_LENGTH bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#ALIGN_1_OFFSET} bytes offset equals to {@link SasFileConstants#ALIGN_1_CHECKER_VALUE},
      * then starting from the {@link SasFileConstants#DATE_CREATED_OFFSET} bytes offset every offset should increase
      * by {@link SasFileConstants#ALIGN_1_VALUE} bytes.
      */
    val Align1Length = 1
    /**
      * If a value with the length of {@link SasFileConstants#ALIGN_1_LENGTH} bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#ALIGN_1_OFFSET} bytes offset equals to {@link SasFileConstants#ALIGN_1_CHECKER_VALUE},
      * then starting from the {@link SasFileConstants#DATE_CREATED_OFFSET} bytes offset every offset should increase by
      * ALIGN_1_VALUE bytes.
      */
    val Align1Value = 4
    /**
      * If a value with the length of {@link SasFileConstants#ALIGN_2_LENGTH} bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#ALIGN_2_OFFSET} bytes offset equals to U64_BYTE_CHECKER_VALUE, then:
      * - this sas7bdat file was created in the 64-bit version of SAS,
      * - starting from the {@link SasFileConstants#SAS_RELEASE_OFFSET} bytes offset every offset should increase by
      * {@link SasFileConstants#ALIGN_2_VALUE} bytes (in addition to {@link SasFileConstants#ALIGN_1_VALUE} bytes, if
      * those are added),
      * - the {@link SasFileConstants#PAGE_COUNT_LENGTH} value should increase by {@link SasFileConstants#ALIGN_2_VALUE}
      * bytes and the number of pages stored at the {@link SasFileConstants#PAGE_COUNT_OFFSET} bytes offset should read
      * as long.
      */
    val U64ByteCheckerValue = 51
    /**
      * If a value with the length of {@link SasFileConstants#ALIGN_2_LENGTH} bytes stored in the sas7bdat file with
      * a ALIGN_2_OFFSET bytes offset equals to {@link SasFileConstants#U64_BYTE_CHECKER_VALUE}, then:
      * - this sas7bdat file was created in the 64-bit version of SAS,
      * - starting from the {@link SasFileConstants#SAS_RELEASE_OFFSET} bytes offset every offset should increase
      * by {@link SasFileConstants#ALIGN_2_VALUE} bytes, (in addition to {@link SasFileConstants#ALIGN_1_VALUE}
      * bytes if those are added) and the number of pages stored at the {@link SasFileConstants#PAGE_COUNT_OFFSET} bytes
      * offset should read as long.
      */
    val Align2Offset = 35L
    /**
      * If a value with the length of ALIGN_2_LENGTH bytes stored in the sas7bdat file at
      * a {@link SasFileConstants#ALIGN_2_OFFSET} bytes offset equals to
      * {@link SasFileConstants#U64_BYTE_CHECKER_VALUE}, then:
      * - this sas7bdat file was created in the 64-bit version of SAS,
      * - starting from the {@link SasFileConstants#SAS_RELEASE_OFFSET} bytes offset every offset should increase by
      * {@link SasFileConstants#ALIGN_2_VALUE} bytes (in addition to {@link SasFileConstants#ALIGN_1_VALUE} bytes if
      * those are added) and the number of pages stored at the {@link SasFileConstants#PAGE_COUNT_OFFSET} bytes offset
      * should read as long.
      */
    val Align2Length = 1
    /**
      * If a value with the length of {@link SasFileConstants#ALIGN_2_LENGTH} bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#ALIGN_2_OFFSET} bytes offset equals to
      * {@link SasFileConstants#U64_BYTE_CHECKER_VALUE}, then:
      * - this sas7bdat file was created in the 64-bit version of SAS,
      * - starting from the {@link SasFileConstants#SAS_RELEASE_OFFSET} bytes offset every offset should increase by
      * ALIGN_2_VALUE bytes (in addition to {@link SasFileConstants#ALIGN_1_VALUE} bytes if those are added) and
      * the number of pages stored at the {@link SasFileConstants#PAGE_COUNT_OFFSET} bytes offset should read as long.
      */
    val Align2Value = 4
    /**
      * If a value with the length of {@link SasFileConstants#ENDIANNESS_LENGTH} bytes stored in the sas7bdat file with
      * a ENDIANNESS_OFFSET bytes offset equals to 1 then the bytes order is little-endian (Intel),
      * if the value equals to 0 then the bytes order is big-endian.
      */
    val EndiannessOffset = 37L
    /**
      * If a value with the length of ENDIANNESS_LENGTH bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#ENDIANNESS_OFFSET} bytes offset equals to 1 then the bytes order is
      * little-endian (Intel), if the value equals to 0 then the bytes order is big-endian.
      */
    val EndiannessLength = 1
    /**
      * If a value with the length of EndiannessLength bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#EndiannessOffset} bytes offset equals to LittleEndianChecker
      * then the bytes order is little-endian (Intel), if the value equals to
      * {@link SasFileConstants#BigEndianChecker} then the bytes order is big-endian.
      */
    val LittleEndianChecker = 1
    /**
      * If a value with the length of EndiannessLength bytes stored in the sas7bdat file with
      * a {@link SasFileConstants#EndiannessOffset} bytes offset equals to
      * {@link SasFileConstants#LittleEndianChecker} then the bytes order is little-endian (Intel),
      * if the value equals to BigEndianChecker then the bytes order is big-endian.
      */
    val BigEndianChecker = 0
    /**
      * The sas7bdat file stores its character encoding with the length of {@link SasFileConstants#ENCODING_LENGTH} bytes
      * and a ENCODING_OFFSET bytes offset.
      */
    val EncodingOffset = 70L
    /**
      * The sas7bdat files its character encoding with the length of ENCODING_LENGTH bytes and
      * a {@link SasFileConstants#ENCODING_OFFSET} bytes offset.
      */
    val EncodingLength = 1

    /** The default encoding to use if one is missing from the sas7bdat files
      */
    val DefaultEncoding = "US-ASCII"

    /**
      * The integer (one or two bytes) at the {@link SasFileConstants#ENCODING_OFFSET} indicates the character encoding
      * of string data.  The SAS_CHARACTER_ENCODINGS map links the values that are known to occur and the associated
      * encoding.  This list excludes encodings present in SAS but missing support in {@link java.nio.charset}
      */
    val SasCharacterEncodings = Map[Byte, String](
        0x46.toByte -> "x-MacArabic",
        0xF5.toByte -> "x-MacCroatian",
        0xF6.toByte -> "x-MacCyrillic",
        0x48.toByte -> "x-MacGreek",
        0x47.toByte -> "x-MacHebrew",
        0xA3.toByte -> "x-MacIceland",
        0x22.toByte -> "ISO-8859-6",
        0x45.toByte -> "x-MacRoman",
        0xF7.toByte -> "x-MacRomania",
        0x49.toByte -> "x-MacThai",
        0x4B.toByte -> "x-MacTurkish",
        0x4C.toByte -> "x-MacUkraine",
        0x7B.toByte -> "Big5",
        0x21.toByte -> "ISO-8859-5",
        0x4E.toByte -> "IBM037",
        0x5F.toByte -> "x-IBM1025",
        0xCF.toByte -> "x-IBM1097",
        0x62.toByte -> "x-IBM1112",
        0x63.toByte -> "x-IBM1122",
        0xB7.toByte -> "IBM01140",
        0xB8.toByte -> "IBM01141",
        0xB9.toByte -> "IBM01142",
        0xBA.toByte -> "IBM01143",
        0xBB.toByte -> "IBM01144",
        0xBC.toByte -> "IBM01145",
        0xBD.toByte -> "IBM01146",
        0xBE.toByte -> "IBM01147",
        0xBF.toByte -> "IBM01148",
        0xD3.toByte -> "IBM01149",
        0x57.toByte -> "IBM424",
        0x58.toByte -> "IBM500",
        0x59.toByte -> "IBM-Thai",
        0x5A.toByte -> "IBM870",
        0x5B.toByte -> "x-IBM875",
        0x7D.toByte -> "GBK",
        0x86.toByte -> "EUC-JP",
        0x8C.toByte -> "EUC-KR",
        0x77.toByte -> "x-EUC-TW",
        0xCD.toByte -> "GB18030",
        0x23.toByte -> "ISO-8859-7",
        0x24.toByte -> "ISO-8859-8",
        0x80.toByte -> "x-IBM1381",
        0x82.toByte -> "x-IBM930",
        0x8B.toByte -> "x-IBM933",
        0x7C.toByte -> "x-IBM935",
        0x75.toByte -> "x-IBM937",
        0x81.toByte -> "x-IBM939",
        0x89.toByte -> "x-IBM942",
        0x8E.toByte -> "x-IBM949",
        0xAC.toByte -> "x-ISO2022-CN-CNS",
        0xA9.toByte -> "x-ISO2022-CN-GB",
        0xA7.toByte -> "ISO-2022-JP",
        0xA8.toByte -> "ISO-2022-KR",
        0x1D.toByte -> "ISO-8859-1",
        0x1E.toByte -> "ISO-8859-2",
        0x1F.toByte -> "ISO-8859-3",
        0x20.toByte -> "ISO-8859-4",
        0x25.toByte -> "ISO-8859-9",
        0xF2.toByte -> "ISO-8859-13",
        0x28.toByte -> "ISO-8859-15",
        0x88.toByte -> "x-windows-iso2022jp",
        0x7E.toByte -> "x-mswin-936",
        0x8D.toByte -> "x-windows-949",
        0x76.toByte -> "x-windows-950",
        0xAD.toByte -> "IBM037",
        0x6C.toByte -> "x-IBM1025",
        0x6D.toByte -> "IBM1026",
        0x6E.toByte -> "IBM1047",
        0xD0.toByte -> "x-IBM1097",
        0x6F.toByte -> "x-IBM1112",
        0x70.toByte -> "x-IBM1122",
        0xC0.toByte -> "IBM01140",
        0xC1.toByte -> "IBM01141",
        0xC2.toByte -> "IBM01142",
        0xC3.toByte -> "IBM01143",
        0xC4.toByte -> "IBM01144",
        0xC5.toByte -> "IBM01145",
        0xC6.toByte -> "IBM01146",
        0xC7.toByte -> "IBM01147",
        0xC8.toByte -> "IBM01148",
        0xD4.toByte -> "IBM01149",
        0x66.toByte -> "IBM424",
        0x67.toByte -> "IBM-Thai",
        0x68.toByte -> "IBM870",
        0x69.toByte -> "x-IBM875",
        0xEA.toByte -> "x-IBM930",
        0xEB.toByte -> "x-IBM933",
        0xEC.toByte -> "x-IBM935",
        0xED.toByte -> "x-IBM937",
        0xEE.toByte -> "x-IBM939",
        0x2B.toByte -> "IBM437",
        0x2C.toByte -> "IBM850",
        0x2D.toByte -> "IBM852",
        0x3A.toByte -> "IBM857",
        0x2E.toByte -> "IBM00858",
        0x2F.toByte -> "IBM862",
        0x33.toByte -> "IBM866",
        0x8A.toByte -> "Shift_JIS",
        0xF8.toByte -> "JIS_X0201",
        0x27.toByte -> "x-iso-8859-11",
        0x1C.toByte -> "US-ASCII",
        0x14.toByte -> "UTF-8",
        0x42.toByte -> "windows-1256",
        0x43.toByte -> "windows-1257",
        0x3D.toByte -> "windows-1251",
        0x3F.toByte -> "windows-1253",
        0x41.toByte -> "windows-1255",
        0x3E.toByte -> "windows-1252",
        0x3C.toByte -> "windows-1250",
        0x40.toByte -> "windows-1254",
        0x44.toByte -> "windows-1258"
    )

    /**
      * The sas7bdat file stores the table name with the length of {@link SasFileConstants#DATASET_LENGTH} bytes and
      * a DATASET_OFFSET bytes offset.
      */
    val DatasetOffset = 92L
    /**
      * The sas7bdat file stores the table name with the length of DATASET_LENGTH bytes and
      * a {@link SasFileConstants#DatasetOffset} bytes offset.
      */
    val DatasetLength = 64
    /**
      * The sas7bdat file stores its file type with the length of {@link SasFileConstants#FILE_TYPE_LENGTH} bytes
      * and a FILE_TYPE_OFFSET bytes offset.
      */
    val FileTypeOffset = 156L
    /**
      * The sas7bdat file stores its file type with the length of FILE_TYPE_LENGTH bytes and
      * a {@link SasFileConstants#FILE_TYPE_OFFSET} bytes offset.
      */
    val FileTypeLength = 8
    /**
      * The sas7bdat file stores its creation date with the length of {@link SasFileConstants#DATE_CREATED_LENGTH} bytes
      * and a DATE_CREATED_OFFSET bytes offset (with possible addition of {@link SasFileConstants#ALIGN_1_VALUE}).
      * The date is a double value denoting the number of seconds elapsed from 01/01/1960 to the date stored.
      */
    val DateCreatedOffset = 164L
    /**
      * The sas7bdat file stores its creation date with the length of DATE_CREATED_LENGTH bytes and
      * a {@link SasFileConstants#DATE_CREATED_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE}). The date is a double value denoting the number of seconds elapsed
      * from 01/01/1960 to the date stored.
      */
    val DateCreatedLength = 8
    /**
      * The sas7bdat file stores its last modification date with the length of
      * {@link SasFileConstants#DATE_MODIFIED_LENGTH} bytes and a DATE_MODIFIED_OFFSET bytes offset (with possible
      * addition of {@link SasFileConstants#ALIGN_1_VALUE}). The date is a double value denoting the number of seconds
      * elapsed from 01/01/1960 to the date stored.
      */
    val DateModifiedOffset = 172L
    /**
      * The sas7bdat file stores its last modification date with the length of DATE_MODIFIED_LENGTH bytes and
      * a {@link SasFileConstants#DATE_MODIFIED_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE}). The date is a value of double format denoting the number of seconds
      * elapsed from 01/01/1960 to the date stored.
      */
    val DateModifiedLength = 8
    /**
      * The sas7bdat file stores the length of its metadata (can be 1024 and 8192) as an int value with the length of
      * {@link SasFileConstants#HEADER_SIZE_LENGTH} bytes and a HEADER_SIZE_OFFSET bytes offset (with possible addition
      * of {@link SasFileConstants#ALIGN_1_VALUE}).
      */
    val HeaderSizeOffset = 196L
    /**
      * The sas7bdat file stores the length of its metadata (can be 1024 and 8192) as an int value with the length of
      * HEADER_SIZE_LENGTH bytes and a  {@link SasFileConstants#HEADER_SIZE_OFFSET} bytes offset (with possible addition
      * of {@link SasFileConstants#ALIGN_1_VALUE}).
      */
    val HeaderSizeLength = 4
    /**
      * The sas7bdat file stores the length of its pages as an int value with the length of
      * {@link SasFileConstants#PAGE_SIZE_LENGTH} bytes and a PAGE_SIZE_OFFSET bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE}).
      */
    val PageSizeOffset = 200L
    /**
      * The sas7bdat file stores the length of its pages as an int value with the length of PAGE_SIZE_LENGTH bytes and
      * a {@link SasFileConstants#PAGE_SIZE_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE}).
      */
    val PageSizeLength = 4
    /**
      * The sas7bdat file stores the number of its pages as an int or long value (depending on
      * {@link SasFileConstants#ALIGN_2_VALUE}) with the length of {@link SasFileConstants#PAGE_COUNT_LENGTH} bytes
      * (with possible addition of {@link SasFileConstants#ALIGN_2_VALUE}) and a PAGE_COUNT_OFFSET bytes offset
      * (with possible addition of {@link SasFileConstants#ALIGN_1_VALUE}).
      */
    val PageCountOffset = 204L
    /**
      * The sas7bdat file stores the number of its pages as an int or long value (depending on
      * {@link SasFileConstants#ALIGN_2_VALUE}) with the length of PAGE_COUNT_LENGTH bytes (with possible addition of
      * {@link SasFileConstants#ALIGN_2_VALUE}) and a {@link SasFileConstants#PAGE_COUNT_OFFSET} bytes offset
      * (with possible addition of {@link SasFileConstants#ALIGN_1_VALUE}).
      */
    val PageCountLength = 4
    /**
      * The sas7bdat file stores the name of SAS version in which the sas7bdat was created with the length of
      * {@link SasFileConstants#SAS_RELEASE_LENGTH} bytes and a SAS_RELEASE_OFFSET bytes offset (with possible addition
      * of {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val SasReleaseOffset = 216L
    /**
      * The sas7bdat file stores the name of SAS version in which the sas7bdat was created with the length of
      * SAS_RELEASE_LENGTH bytes and a {@link SasFileConstants#SAS_RELEASE_OFFSET} bytes offset (with possible addition
      * of {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val SasReleaseLength = 8
    /**
      * The sas7bdat file stores the name of the server version on which the sas7bdat was created with the length of
      * {@link SasFileConstants#SAS_SERVER_TYPE_LENGTH} bytes and a SAS_SERVER_TYPE_OFFSET bytes offset (with possible
      * addition of {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val SasServerTypeOffset = 224L
    /**
      * The sas7bdat file stores the name of the server version on which the sas7bdat was created with the length of
      * SAS_SERVER_TYPE_LENGTH bytes and a {@link SasFileConstants#SAS_SERVER_TYPE_OFFSET} bytes offset (with possible
      * addition of {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val SasServerTypeLength = 16
    /**
      * The sas7bdat file stores the version of the OS in which the sas7bdat was created with the length of
      * {@link SasFileConstants#OS_VERSION_NUMBER_LENGTH} bytes and a OS_VERSION_NUMBER_OFFSET bytes offset
      * (with possible addition of {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val OsVersionNumberOffset = 240L
    /**
      * The sas7bdat file stores the version of the OS in which the sas7bdat was created with the length of
      * OS_VERSION_NUMBER_LENGTH bytes and a {@link SasFileConstants#OS_VERSION_NUMBER_OFFSET} bytes offset (with
      * possible addition of {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val OsVersionNumberLength = 16
    /**
      * The sas7bdat file stores the name of the OS in which the sas7bdat was created with the length of
      * {@link SasFileConstants#OS_MAKER_LENGTH} bytes and a OS_MAKER_OFFSET bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}). If the OS name is
      * an empty string, then the file stores the OS name with the length of {@link SasFileConstants#OS_NAME_LENGTH}
      * bytes and a {@link SasFileConstants#OS_NAME_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val OsMarkerOffset = 256L
    /**
      * The sas7bdat file stores the name of the OS in which the sas7bdat was created with the length of OS_MAKER_LENGTH
      * bytes and a {@link SasFileConstants#OS_MAKER_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}). If the OS name is
      * an empty string, then the file stores the OS name with the length of {@link SasFileConstants#OS_NAME_LENGTH}
      * bytes and a {@link SasFileConstants#OS_NAME_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val OsMarkerLength = 16
    /**
      * The sas7bdat file stores the name of the OS in which the sas7bdat was created with the length of
      * {@link SasFileConstants#OS_NAME_LENGTH} bytes and a OS_NAME_OFFSET bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}). If the OS name is
      * an empty string, then the file stores the OS name with the length of {@link SasFileConstants#OS_MAKER_LENGTH}
      * bytes and a {@link SasFileConstants#OS_MAKER_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val OsNameOffset = 272L
    /**
      * The sas7bdat file stores the name of the OS in which the sas7bdat was created with the length of OS_NAME_LENGTH
      * bytes and a {@link SasFileConstants#OS_NAME_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}). If the OS name is
      * an empty string, then the file stores the OS name with the length of  {@link SasFileConstants#OS_MAKER_LENGTH}
      * bytes and a {@link SasFileConstants#OS_MAKER_OFFSET} bytes offset (with possible addition of
      * {@link SasFileConstants#ALIGN_1_VALUE} and {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val OsNameLength = 16
    /**
      * An offset in bytes from the start of the page - for sas7bdat files created in the 32-bit version of SAS
      * (see {@link SasFileConstants#ALIGN_2_VALUE}). Added to all offsets within a page.
      */
    val PageBitOffsetX86 = 16
    /**
      * An offset in bytes from the start of the page - for sas7bdat files created in the 64-bit version of SAS
      * (see {@link SasFileConstants#ALIGN_2_VALUE}). Added to all offsets within a page.
      */
    val PageBitOffsetX64 = 32
    /**
      * The length in bytes of one subheader pointer ({@link SasFileParser.SubheaderPointer}) of a sas7bdat file
      * created in the 32-bit version of SAS (see {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val SubheaderPointerLengthX86 = 12
    /**
      * The length in bytes of one subheader pointer ({@link SasFileParser.SubheaderPointer}) of a sas7bdat file
      * created in the 64-bit version of SAS (see {@link SasFileConstants#ALIGN_2_VALUE}).
      */
    val SubheaderPointerLengthX64 = 24
    /**
      * The sas7bdat file stores the type of page as a short value with the length of
      * {@link SasFileConstants#PAGE_TYPE_LENGTH} bytes and a PAGE_TYPE_OFFSET bytes offset (with addition of
      * {@link SasFileConstants#PAGE_BIT_OFFSET_X86} or {@link SasFileConstants#PAGE_BIT_OFFSET_X64}).
      * There can be {@link SasFileConstants#PAGE_META_TYPE_1}, {@link SasFileConstants#PAGE_META_TYPE_2},
      * {@link SasFileConstants#PAGE_DATA_TYPE}, or {@link SasFileConstants#PAGE_MIX_TYPE} page types.
      */
    val PAGE_TYPE_OFFSET = 0L
    /**
      * The sas7bdat file stores the type of page as a short value with the length of PAGE_TYPE_LENGTH bytes
      * and a {@link SasFileConstants#PAGE_TYPE_OFFSET} bytes offset (with addition of
      * {@link SasFileConstants#PAGE_BIT_OFFSET_X86} or {@link SasFileConstants#PAGE_BIT_OFFSET_X64}).
      * There can be {@link SasFileConstants#PAGE_META_TYPE_1}, {@link SasFileConstants#PAGE_META_TYPE_2},
      * {@link SasFileConstants#PAGE_DATA_TYPE}, or {@link SasFileConstants#PAGE_MIX_TYPE} page types.
      */
    val PAGE_TYPE_LENGTH = 2
    /**
      * For pages of the {@link SasFileConstants#PAGE_DATA_TYPE} type, the sas7bdat file stores the number of rows
      * in the table on the current page as a short value - with the length of
      * {@link SasFileConstants#BLOCK_COUNT_LENGTH} bytes and a BLOCK_COUNT_OFFSET bytes offset (with addition of
      * {@link SasFileConstants#PAGE_BIT_OFFSET_X86} or {@link SasFileConstants#PAGE_BIT_OFFSET_X64}).
      */
    val BLOCK_COUNT_OFFSET = 2L
    /**
      * For pages of the {@link SasFileConstants#PAGE_DATA_TYPE} type, the sas7bdat file stores the number of rows
      * in the table on the current page as a short value - with the length of BLOCK_COUNT_LENGTH bytes
      * and a {@link SasFileConstants#BLOCK_COUNT_OFFSET} bytes offset (with addition of
      * {@link SasFileConstants#PAGE_BIT_OFFSET_X86} or {@link SasFileConstants#PAGE_BIT_OFFSET_X64}).
      */
    val BLOCK_COUNT_LENGTH = 2
    /**
      * For pages of the {@link SasFileConstants#PAGE_META_TYPE_1}, {@link SasFileConstants#PAGE_META_TYPE_2}
      * and {@link SasFileConstants#PAGE_MIX_TYPE} types, the sas7bdat file stores the number of subheaders on
      * the current page as a short value - with the length of {@link SasFileConstants#SUBHEADER_COUNT_LENGTH}
      * bytes and a SUBHEADER_COUNT_OFFSET bytes offset from the beginning of the page (with addition of
      * {@link SasFileConstants#PAGE_BIT_OFFSET_X86} or {@link SasFileConstants#PAGE_BIT_OFFSET_X64}).
      */
    val SUBHEADER_COUNT_OFFSET = 4L
    /**
      * For pages of the {@link SasFileConstants#PAGE_META_TYPE_1}, {@link SasFileConstants#PAGE_META_TYPE_2}
      * and {@link SasFileConstants#PAGE_MIX_TYPE} types, the sas7bdat file stores the number of subheaders on
      * the current page as a short value - with the length of SUBHEADER_COUNT_LENGTH bytes and a
      * {@link SasFileConstants#SUBHEADER_COUNT_OFFSET} bytes offset from the beginning of the page
      * (with addition of {@link SasFileConstants#PAGE_BIT_OFFSET_X86} or {@link SasFileConstants#PAGE_BIT_OFFSET_X64}).
      */
    val SUBHEADER_COUNT_LENGTH = 2
    /**
      * The page type storing metadata as a set of subheaders. It can also store compressed row data in subheaders.
      * The sas7bdat format has two values that correspond to the page type 'meta'.
      */
    val PageMetaType1 = 0
    val PageMetaType2 = 16384
    /**
      * The page type storing only data as a number of table rows.
      */
    val PageDataType = 256
    /**
      * The page type storing metadata as a set of subheaders and data as a number of table rows.
      */
    val PageMixType = 512
    /**
      * The page type amd.
      */
    val PageAmdType = 1024
    /**
      * The sas7bdat file stores the array of subheader pointers ({@link SasFileParser.SubheaderPointer}) at this
      * offset (adding {@link SasFileConstants#PAGE_BIT_OFFSET_X86} or {@link SasFileConstants#PAGE_BIT_OFFSET_X64})
      * from the beginning of the page.
      */
    val SubheaderPointersOffset = 8
    /**
      * If the {@link SasFileParser.SubheaderPointer#compression} value of a subheader equals to TRUNCATED_SUBHEADER_ID
      * then it does not contain useful information.
      */
    val TruncatedSubheaderId = 1
    /**
      * A subheader with compressed data has two parameters:
      * its {@link SasFileParser.SubheaderPointer#compression} should equal to COMPRESSED_SUBHEADER_ID and its
      * {@link SasFileParser.SubheaderPointer#type} should equal to {@link SasFileConstants#COMPRESSED_SUBHEADER_TYPE}.
      */
    val CompressedSubheaderId = 4
    /**
      * A Subheader with compressed data has two parameters:
      * its {@link SasFileParser.SubheaderPointer#compression} should equal to
      * {@link SasFileConstants#COMPRESSED_SUBHEADER_ID} and its {@link SasFileParser.SubheaderPointer#type}
      * should equal to COMPRESSED_SUBHEADER_TYPE.
      */
    val CompressedSubheaderType = 1
    /**
      * The number of bits in a byte.
      */
    val BitsInByte = 8
    /**
      * The multiplier whose product with the length of the variable type (that can be int or long depending on the
      * {@link SasFileConstants#ALIGN_2_VALUE} value) is the offset from the subheader beginning
      * {@link SasFileParser.RowSizeSubheader} at which the row length is stored.
      */
    val ROW_LENGTH_OFFSET_MULTIPLIER = 5
    /**
      * The multiplier whose product with the length of the variable type (that can be int or long depending on the
      * {@link SasFileConstants#ALIGN_2_VALUE} value) is the offset from the subheader beginning
      * {@link SasFileParser.RowSizeSubheader} at which the number of rows in the table is stored.
      */
    val ROW_COUNT_OFFSET_MULTIPLIER = 6
    /**
      * The multiplier whose product with the length of the variable type (that can be int or long depending on the
      * {@link SasFileConstants#ALIGN_2_VALUE} value) is the offset from the subheader beginning
      * {@link SasFileParser.RowSizeSubheader} at which the file stores the number of rows in the table
      * on the last page of the {@link SasFileConstants#PAGE_MIX_TYPE} type.
      */
    val ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER = 15
    /**
      * The number of bytes taken by the value denoting the length of the text block with information about
      * file compression and table rows (name, label, format).
      */
    val TextBlockSizeLength = 2
    /**
      * A substring that appears in the text block with information about file compression and table rows
      * (name, label, format) if Run Length Encoding is used.
      */
    val CompressCharIdentifyingString = "SASYZCRL"
    /**
      * A substring that appears in the text block with information about file compression and table rows
      * (name, label, format) if Ross Data compression is used.
      */
    val CompressBinIdentifyingString = "SASYZCR2"
    /**
      * The length of the column name pointer in bytes.
      */
    val ColumnNamePointerLength = 8
    /**
      * For each table column, the sas7bdat file stores the index of the
      * {@link SasFileParser.ColumnTextSubheader} subheader whose text block contains the name
      * of the column - with the length of {@link SasFileConstants#COLUMN_NAME_TEXT_SUBHEADER_LENGTH} bytes and an offset
      * measured from the beginning of the {@link SasFileParser.ColumnNameSubheader} subheader
      * and calculated by the following formula: COLUMN_NAME_TEXT_SUBHEADER_OFFSET +
      * + column number * {@link SasFileConstants#COLUMN_NAME_POINTER_LENGTH} + size of the value type (int or long
      * depending on the {@link SasFileConstants#ALIGN_2_VALUE} value).
      */
    val ColumnNameTextSubheaderOffset = 0L
    /**
      * For each table column, the sas7bdat file stores the index of the
      * {@link SasFileParser.ColumnTextSubheader} subheader whose text block contains the name
      * of the column - with the length of COLUMN_NAME_TEXT_SUBHEADER_LENGTH bytes and an offset measured from
      * the beginning of the {@link SasFileParser.ColumnNameSubheader} subheader
      * and calculated by the following formula: {@link SasFileConstants#COLUMN_NAME_TEXT_SUBHEADER_OFFSET} +
      * + column number * {@link SasFileConstants#COLUMN_NAME_POINTER_LENGTH} + size of the value type (int or long
      * depending on the {@link SasFileConstants#ALIGN_2_VALUE} value).
      */
    val ColumnNameTextSubheaderLength = 2
    /**
      * For each table column, the sas7bdat file stores the offset (in symbols) of the column name from the beginning
      * of the text block of the {@link SasFileParser.ColumnTextSubheader} subheader (see
      * {@link SasFileConstants#COLUMN_NAME_TEXT_SUBHEADER_OFFSET})- with the length of
      * {@link SasFileConstants#COLUMN_NAME_OFFSET_LENGTH} bytes and an offset measured from the beginning
      * of the {@link SasFileParser.ColumnNameSubheader} subheader and calculated by
      * the following formula: COLUMN_NAME_OFFSET_OFFSET +
      * + column number * {@link SasFileConstants#COLUMN_NAME_POINTER_LENGTH} + size of the value type (int or long
      * depending on the {@link SasFileConstants#ALIGN_2_VALUE} value).
      */
    val ColumnNameOffsetOffset = 2L
    /**
      * For each table column, the sas7bdat file stores the offset (in symbols) of the column name from the beginning
      * of the text block of the {@link SasFileParser.ColumnTextSubheader} subheader (see
      * {@link SasFileConstants#COLUMN_NAME_TEXT_SUBHEADER_OFFSET})- with the length of COLUMN_NAME_OFFSET_LENGTH bytes
      * and an offset measured from the beginning of the {@link SasFileParser.ColumnNameSubheader}
      * subheader and calculated by the following formula: {@link SasFileConstants#COLUMN_NAME_OFFSET_OFFSET} +
      * + column number * {@link SasFileConstants#COLUMN_NAME_POINTER_LENGTH} + size of the value type (int or long
      * depending on the {@link SasFileConstants#ALIGN_2_VALUE} value).
      */
    val ColumnNameOffsetLength = 2
    /**
      * For each table column, the sas7bdat file stores column name length (in symbols):
      * - with the length of {@link SasFileConstants#COLUMN_NAME_LENGTH_LENGTH} bytes,
      * - at an offset  measured from the beginning of the
      * {@link SasFileParser.ColumnNameSubheader} subheader
      * and calculated by the following formula: COLUMN_NAME_LENGTH_OFFSET +
      * + column number * {@link SasFileConstants#COLUMN_NAME_POINTER_LENGTH} + size of the value type (int or long
      * depending on the {@link SasFileConstants#ALIGN_2_VALUE} value).
      */
    val ColumnNameLengthOffset = 4L
    /**
      * For each table column, the sas7bdat file stores column name length (in symbols):
      * - with the length of COLUMN_NAME_LENGTH_LENGTH bytes.
      * - at an offset measured from the beginning of the
      * {@link SasFileParser.ColumnNameSubheader} subheader and calculated
      * by the following formula: {@link SasFileConstants#COLUMN_NAME_LENGTH_OFFSET} +
      * + column number * {@link SasFileConstants#COLUMN_NAME_POINTER_LENGTH} + size of the value type (int or long
      * depending on the {@link SasFileConstants#ALIGN_2_VALUE} value).
      */
    val ColumnNameLengthLength = 2
    /**
      * For every table column, the sas7bdat file stores the value (int or long depending on
      * {@link SasFileConstants#ALIGN_2_VALUE}) that defines the offset of data in the current column
      * from the beginning of the row with data in bytes:
      * - at an offset measured from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader
      * and calculated by the following formula: COLUMN_DATA_OFFSET_OFFSET +
      * + column index * (8 + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})) +
      * + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader.
      */
    val ColumnDataOffsetOffset = 8L
    /**
      * For every table column, the sas7bdat file stores the denotation (in bytes) of data length in a column:
      * - at an offset measured from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader
      * and calculated by the following formula: COLUMN_DATA_LENGTH_OFFSET +
      * + column index * (8 + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})) +
      * + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader,
      * - with the length of {@link SasFileConstants#COLUMN_DATA_LENGTH_LENGTH} bytes.
      */
    val ColumnDataLengthOffset = 8L
    /**
      * For every table column, the sas7bdat file stores the denotation (in bytes) of data length in a column:
      * - at an offset measured from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader
      * and calculated by the following formula: {@link SasFileConstants#COLUMN_DATA_LENGTH_OFFSET} +
      * + column index * (8 + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})) +
      * + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader,
      * - with the length of COLUMN_DATA_LENGTH_LENGTH bytes.
      */
    val ColumnDataLengthLength = 4
    /**
      * For every table column, the sas7bdat file stores the data type of a column:
      * - with the length of {@link SasFileConstants#COLUMN_TYPE_LENGTH} bytes.
      * - at an offset measured from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader
      * and calculated by the following formula: COLUMN_TYPE_OFFSET +
      * + column index * (8 + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})) +
      * + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader,
      * If type=1, then the column stores numeric values, if type=0, the column stores text.
      */
    val ColumnTypeOffset = 14L
    /**
      * For every table column, the sas7bdat file stores the data type of a column:
      * - with the length of COLUMN_TYPE_LENGTH bytes.
      * - at an offset measured from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader
      * and calculated by the following formula: {@link SasFileConstants#COLUMN_TYPE_OFFSET} +
      * + column index * (8 + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})) +
      * + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.ColumnAttributesSubheader} subheader,
      * If type=1, then the column stores numeric values, if type=0, the column stores text.
      */
    val ColumnTypeLength = 1
    /**
      * For some table column, the sas7bdat file stores width of format:
      * - with the length of {@link SasFileConstants#COLUMN_FORMAT_WIDTH_OFFSET_LENGTH} bytes,
      * - at an offset calculated as COLUMN_FORMAT_WIDTH_OFFSET bytes + the size of value types
      * (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatWidthOffset = 8L
    /**
      * For some table column, the sas7bdat file stores width of format:
      * - with the length of COLUMN_FORMAT_WIDTH_OFFSET_LENGTH bytes,
      * - at an offset calculated as {@link SasFileConstants#COLUMN_FORMAT_WIDTH_OFFSET bytes} + the size of value types
      * (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatWidthOffsetLength = 2
    /**
      * For some table column, the sas7bdat file stores precision of format:
      * - with the length of {@link SasFileConstants#COLUMN_FORMAT_PRECISION_OFFSET_LENGTH} bytes,
      * - at an offset calculated as COLUMN_FORMAT_PRECISION_OFFSET bytes + the size of value types
      * (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatPrecisionOffset = 10L
    /**
      * For some table column, the sas7bdat file stores width of format:
      * - with the length of COLUMN_FORMAT_PRECISION_OFFSET_LENGTH bytes,
      * - at an offset calculated as {@link SasFileConstants#COLUMN_FORMAT_PRECISION_OFFSET bytes} +
      * + the size of value type (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatPrecisionOffsetLength = 2
    /**
      * For every table column, the sas7bdat file stores the index of
      * the {@link SasFileParser.ColumnTextSubheader} whose text block stores the column format:
      * - with the length of {@link SasFileConstants#COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_LENGTH} bytes,
      * - at an offset calculated as COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET bytes +
      * + 3 * the size of value types (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatTextSubheaderIndexOffset = 22L
    /**
      * For every table column, the sas7bdat file stores the index of the
      * {@link SasFileParser.ColumnTextSubheader} whose text block stores the column format:
      * - with the length of COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_LENGTH bytes,
      * - at an offset calculated as {@link SasFileConstants#COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET} bytes +
      * + 3 * the size of value types (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatTextSubheaderIndexLength = 2
    /**
      * For every table column, the sas7bdat file stores the offset (in symbols) of the column format from
      * the beginning of the text block of the {@link SasFileParser.ColumnTextSubheader} subheader
      * where it belongs:
      * - with the length of {@link SasFileConstants#COLUMN_FORMAT_OFFSET_LENGTH} bytes,
      * - at an offset calculated as COLUMN_FORMAT_OFFSET_OFFSET bytes + 3 * the size of value types
      * (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from the beginning of
      * the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatOffsetOffset = 24L
    /**
      * For every table column, the sas7bdat file stores the offset (in symbols) of the column format from
      * the beginning of the text block of the {@link SasFileParser.ColumnTextSubheader}
      * subheader where it belongs:
      * - with the length of COLUMN_FORMAT_OFFSET_LENGTH bytes,
      * - at an offset calculated as {@link SasFileConstants#COLUMN_FORMAT_OFFSET_OFFSET} bytes +
      * + 3 * the size of value types (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatOffsetLength = 2
    /**
      * For every table column, the sas7bdat file stores the column format length (in symbols):
      * - with the length of {@link SasFileConstants#COLUMN_FORMAT_LENGTH_LENGTH} bytes,
      * - at an offset calculated as COLUMN_FORMAT_LENGTH_OFFSET bytes + the size of three value types
      * (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from the beginning of
      * the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatLengthOffset = 26L

    /**
      * For every table column, the sas7bdat file stores the column format length (in symbols):
      * - with the length of COLUMN_FORMAT_LENGTH_LENGTH bytes,
      * - at an offset calculated as {@link SasFileConstants#COLUMN_FORMAT_LENGTH_OFFSET} bytes +
      * 3 * the size of value types (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnFormatLengthLength = 2

    /**
      * For every table column, the sas7bdat file stores the index of the
      * {@link SasFileParser.ColumnTextSubheader} subheader
      * whose text block contains the column label:
      * - with the length of {@link SasFileConstants#COLUMN_LABEL_TEXT_SUBHEADER_INDEX_LENGTH} bytes,
      * - at an offset calculated as COLUMN_LABEL_TEXT_SUBHEADER_INDEX_OFFSET bytes +
      * + 3 * the size of value types (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnLabelTextSubheaderIndexOffset = 28L
    /**
      * For every table column, the sas7bdat file stores the index of the
      * {@link SasFileParser.ColumnTextSubheader} subheader
      * whose text block contains the column label:
      * - with the length of COLUMN_LABEL_TEXT_SUBHEADER_INDEX_LENGTH bytes,
      * - at an offset equal to {@link SasFileConstants#COLUMN_LABEL_TEXT_SUBHEADER_INDEX_OFFSET} bytes +
      * + 3 * the size of value types (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnLabelTextSubheaderIndexLength = 2
    /**
      * For every table column, the sas7bdat file stores the column label`s offset (in symbols) from the beginning of
      * the text block where it belongs (see {@link SasFileConstants#COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET}):
      * - with the length of {@link SasFileConstants#COLUMN_LABEL_OFFSET_LENGTH} bytes.
      * - at an offset equal to COLUMN_LABEL_OFFSET_OFFSET bytes + 3 * the size of value types (int or long
      * depending on {@link SasFileConstants#ALIGN_2_VALUE}) from the beginning of
      * the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      **/
    val ColumnLabelOffsetOffset = 30L
    /**
      * For every table column, the sas7bdat file stores the column label`s offset (in symbols) from the beginning of
      * the text block where it belongs (see {@link SasFileConstants#COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET}):
      * - with the length of COLUMN_LABEL_OFFSET_LENGTH bytes.
      * - at an offset equal to {@link SasFileConstants#COLUMN_LABEL_OFFSET_OFFSET} bytes + 3 * the size
      * of value types(int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from the beginning of the
      * {@link SasFileParser.FormatAndLabelSubheader} subheader.
      **/
    val ColumnLabelOffsetLength = 2
    /**
      * For every table column, the sas7bdat file stores the length of the column label (in symbols):
      * - with the length of {@link SasFileConstants#COLUMN_LABEL_LENGTH_LENGTH} bytes.
      * - at an offset calculated as COLUMN_LABEL_LENGTH_OFFSET bytes +
      * 3 * the size of value types (int or long depending on {@link SasFileConstants#ALIGN_2_VALUE})
      * from the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnLabelLengthOffset = 32L
    /**
      * For every table column, the sas7bdat file stores the length of the column label (in symbols):
      * - with the length of COLUMN_LABEL_LENGTH_LENGTH bytes.
      * - at an offset calculated as {@link SasFileConstants#COLUMN_LABEL_LENGTH_OFFSET} bytes +
      * 3 * the size of value types(int or long depending on {@link SasFileConstants#ALIGN_2_VALUE}) from
      * the beginning of the {@link SasFileParser.FormatAndLabelSubheader} subheader.
      */
    val ColumnLabelLengthLength = 2
    /**
      * Accuracy to define whether the numeric result of {@link SasFileParser#convertByteArrayToNumber(byte[])} is
      * a long or double value.
      */
    val EPSILON = 1E-14
    /**
      * Accuracy to define whether the numeric result of {@link SasFileParser#convertByteArrayToNumber(byte[])} is NAN.
      */
    val NAN_EPSILON = 1E-300
    /**
      * The number of milliseconds in a second.
      */
    val MILLISECONDS_IN_SECONDS = 1000L
    /**
      * The number of seconds in a minute.
      */
    val SecondsInMinute = 60
    /**
      * The number of minutes in an hour.
      */
    val MinutesInHour = 60
    /**
      * The number of hours in a day.
      */
    val HoursInDay = 24
    /**
      * The number of days in a non-leap year.
      */
    val DaysInYear = 365
    /**
      * The difference in days between 01/01/1960 (the dates starting point in SAS) and 01/01/1970 (the dates starting
      * point in Java).
      */
    val StartDatesDaysDifference = DaysInYear * 10 + 3
    /**
      * The difference in seconds between 01/01/1960 (the dates starting point in SAS) and 01/01/1970 (the dates starting
      * point in Java).
      */
    val StartDatesSecondsDifference = SecondsInMinute * MinutesInHour * HoursInDay * StartDatesDaysDifference
    /**
      * The date formats to store the day, month, and year. Appear in the data of the
      * {@link SasFileParser.FormatAndLabelSubheader} subheader and are stored in {@link com.epam.parso.Column#format}.
      */
    val DateFormatStrings = Set(
      "B8601DA",
      "E8601DA",
      "DATE",
      "DAY",
      "DDMMYY",
      "DDMMYYB",
      "DDMMYYC",
      "DDMMYYD",
      "DDMMYYN",
      "DDMMYYP",
      "DDMMYYS",
      "WEEKDATE",
      "WEEKDATX",
      "WEEKDAY",
      "DOWNAME",
      "WORDDATE",
      "WORDDATX",
      "YYMM",
      "YYMMC",
      "YYMMD",
      "YYMMN",
      "YYMMP",
      "YYMMS",
      "YYMMDD",
      "YYMMDDB",
      "YYMMDDC",
      "YYMMDDD",
      "YYMMDDN",
      "YYMMDDP",
      "YYMMDDS",
      "YYMON",
      "YEAR",
      "JULDAY",
      "JULIAN",
      "MMDDYY",
      "MMDDYYC",
      "MMDDYYD",
      "MMDDYYN",
      "MMDDYYP",
      "MMDDYYS",
      "MMYY",
      "MMYYC",
      "MMYYD",
      "MMYYN",
      "MMYYP",
      "MMYYS",
      "MONNAME",
      "MONTH",
      "MONYY")

    /**
      * The date formats to store the day, month, year, hour, minutes, seconds, and milliseconds.
      * Appear in the data of the {@link SasFileParser.FormatAndLabelSubheader} subheader
      * and are stored in {@link com.epam.parso.Column#format}.
      */
    val DateTimeFormatStrings = Set(
      "B8601DN",
      "B8601DT",
      "B8601DX",
      "B8601DZ",
      "B8601LX",
      "E8601DN",
      "E8601DT",
      "E8601DX",
      "E8601DZ",
      "E8601LX",
      "DATEAMPM",
      "DATETIME",
      "DTDATE",
      "DTMONYY",
      "DTWKDATX",
      "DTMONYY",
      "DTWKDATX",
      "DTYEAR",
      "TOD",
      "MDYAMPM")
}
