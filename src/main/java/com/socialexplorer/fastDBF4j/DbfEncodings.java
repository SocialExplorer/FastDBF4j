package com.socialexplorer.fastDBF4j;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Maida
 * Date: 11/12/14
 * Time: 3:27 PM
 */
public enum DbfEncodings {
    // (Language driver -> Code page) mapping: http://webhelp.esri.com/arcpad/8.0/referenceguide/index.htm#locales/task_code.htm
    // TODO Check other sources for this mapping.

    CP437((byte) 0x01, "437", "U.S. MS-DOS"),
    CP850((byte) 0x02, "850", "International MS-DOS"),
    CP1252((byte) 0x03, "1252", "Window ANSI"),
    CP865((byte) 0x08, "865", "Danish OEM"),
    CP437_DUTCH((byte) 0x09, "437", "Dutch OEM"),
    CP850_DUTCH((byte) 0x0A, "850", "Dutch OEM*"),
    CP437_FINNISH((byte) 0x0B, "437", "Finnish OEM"),
    CP437_FRENCH((byte) 0x0D, "437", "French OEM"),
    CP850_FRENCH((byte) 0x0E, "850", "French OEM*"),
    CP437_GERMAN((byte) 0x0F, "437", "German OEM"),
    CP850_GERMAN((byte) 0x10, "850", "German OEM*"),
    CP437_ITALIAN((byte) 0x11, "437", "Italian OEM"),
    CP850_ITALIAN((byte) 0x12, "850", "Italian OEM*"),
    CP932_JAPANESE((byte) 0x13, "932", "Japanese Shift-JIS"),
    CP850_SPANISH((byte) 0x14, "850", "Spanish OEM*"),
    CP437_SWEDISH((byte) 0x15, "437", "Swedish OEM"),
    CP850_SWEDISH((byte) 0x16, "850", "Swedish OEM*"),
    CP865_NORWEGIAN((byte) 0x17, "865", "Norwegian OEM"),
    CP437_SPANISH((byte) 0x18, "437", "Spanish OEM"),
    CP437_ENGLISH((byte) 0x19, "437", "English OEM (Britain)"),
    CP950_ENGLISH((byte) 0x1A, "850", "English OEM (Britain)*"),
    CP437_ENGLISH_US((byte) 0x1B, "437", "English OEM (U.S.)"),
    CP863_FRENCH((byte) 0x1C, "863", "French OEM (Canada)"),
    CP850_FRENCH_2((byte) 0x1D, "850", "French OEM*"),
    CP852_CZECH((byte) 0x1F, "852", "Czech OEM"),
    CP852_HUNGARIAN((byte) 0x22, "852", "Hungarian OEM"),
    CP852_POLISH((byte) 0x23, "852", "Polish OEM"),
    CP860_PORTUGUESE((byte) 0x24, "860", "Portugese OEM"),
    CP850_PORTUGUESE((byte) 0x25, "850", "Potugese OEM*"),
    CP866_RUSSIAN((byte) 0x26, "866", "Russian OEM"),
    CP850_ENGLISH((byte) 0x37, "850", "English OEM (U.S.)*"),
    CP852_ROMANIAN((byte) 0x40, "852", "Romanian OEM"),
    CP936_CHINESE((byte) 0x4D, "936", "Chinese GBK (PRC)"),
    CP949_KOREAN((byte) 0x4E, "949", "Korean (ANSI/OEM)"),
    CP950_CHINESE((byte) 0x4F, "950", "Chinese Big 5 (Taiwan)"),
    CP874_THAI((byte) 0x50, "874", "Thai (ANSI/OEM)"),
    CP1252_ANSI((byte) 0x57, "1252", "ANSI"),
    CP1252_WESTERN_EUROPEAN_ANSI((byte) 0x58, "1252", "Western European ANSI"),
    CP1252_SPANISH_ANSI((byte) 0x59, "1252", "Spanish ANSI"),
    CP852((byte) 0x64, "852", "Eastern European MS-DOS"),
    CP866((byte) 0x65, "866", "Russian MS-DOS"),
    CP865_NORDIC((byte) 0x66, "865", "Nordic MS-DOS"),
    CP861((byte) 0x67, "861", "Icelandic MS-DOS"),
    CP737_GREEK((byte) 0x6A, "737", "Greek MS-DOS (437G)"),
    CP857_TURKISH((byte) 0x6B, "857", "Turkish MS-DOS"),
    CP863((byte) 0x6C, "863", "French-Canadian MS-DOS"),
    CP950((byte) 0x78, "950", "Taiwan Big 5"),
    CP949((byte) 0x79, "949", "Hangul (Wansung)"),
    CP936((byte) 0x7A, "936", "PRC GBK"),
    CP932((byte) 0x7B, "932", "Japanese Shift-JIS"),
    CP874((byte) 0x7C, "874", "Thai Windows/MS-DOS"),
    CP737((byte) 0x86, "737", "Greek OEM"),
    C852((byte) 0x87, "852", "Slovenian OEM"),
    CP857((byte) 0x88, "857", "Turkish OEM"),
    CP1250((byte) 0xC8, "1250", "Eastern European Windows"),
    CP1251((byte) 0xC9, "1251", "Russian Windows"),
    CP1254((byte) 0xCA, "1254", "Turkish Windows"),
    CP1253((byte) 0xCB, "1253", "Greek Windows"),
    CP1257((byte) 0xCC, "1257", "Baltic Windows"),
    ;

    private byte dbfLanguageDriverId;
    private String codePage;
    private String description;

    private DbfEncodings(byte dbfLanguageDriverId, String codePage, String description) {
        this.dbfLanguageDriverId = dbfLanguageDriverId;
        this.codePage = codePage;
        this.description = description;
    }

    public byte getDbfLanguageDriverId() {
        return dbfLanguageDriverId;
    }

    public void setDbfLanguageDriverId(byte dbfLanguageDriverId) {
        this.dbfLanguageDriverId = dbfLanguageDriverId;
    }

    public String getCodePage() {
        return codePage;
    }

    public void setCodePage(String codePage) {
        this.codePage = codePage;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public static Map getDbfEncodingsMap() {
        return dbfEncodingsMap;
    }

    public static void setDbfEncodingsMap(Map dbfEncodingsMap) {
        DbfEncodings.dbfEncodingsMap = dbfEncodingsMap;
    }

    private static Map<Byte, String> dbfEncodingsMap = new HashMap();
    static {
        for (DbfEncodings dbfEncoding : EnumSet.allOf(DbfEncodings.class)) {
            dbfEncodingsMap.put(dbfEncoding.getDbfLanguageDriverId(), dbfEncoding.getCodePage());
        }
    }

    public static String getCodePageFromLanguageId(byte dbfLanguageDriverId) {
        return dbfEncodingsMap.get(dbfLanguageDriverId);
    }
}
