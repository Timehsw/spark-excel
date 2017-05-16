package com.hsw.excel;

/**
 * Created by HuShiwei on 2016/7/29 0029.
 */

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * 解析xls文件，利用poi对文件进行解析
 * Excel解析类，读取每一行并转成字符串输出以换行符分割，每一行的字段以\t分割，最终是一串字符串
 */
public class ExcelParser {
    private static final Logger logger = LoggerFactory.getLogger(ExcelParser.class);
    private StringBuilder currentString = null;
    private long bytesRead = 0;

    public String parseExcelData(InputStream is) {
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(is);
            HSSFSheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();
            currentString = new  StringBuilder() ;
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    if (null!=cell) {
                        switch (cell.getCellType()) {
                            case Cell.CELL_TYPE_BOOLEAN: // Boolean
                                bytesRead++;
                                currentString.append(cell.getBooleanCellValue() + "\t");
                                break;
                            case Cell.CELL_TYPE_NUMERIC:  // 数字
                                bytesRead++;
                                currentString.append(cell.getNumericCellValue() + "\t");
                                break;
                            case Cell.CELL_TYPE_STRING:  // 字符串
                                bytesRead++;
                                currentString.append(cell.getStringCellValue() + "\t");
                                break;
                            case Cell.CELL_TYPE_FORMULA:
                                bytesRead++;
                                currentString.append(cell.getCellFormula() + "\t");
                                break;
                            case Cell.CELL_TYPE_BLANK:
                                bytesRead++;// 空白字符
                                currentString.append("" + "\t");
                                break;
                            case Cell.CELL_TYPE_ERROR:
                                bytesRead++;//  错误
                                currentString.append("非法字符" + "\t");
                                break;
                            default:
                                bytesRead++;
                                currentString.append("未知类型" + "\t");
                                break;
                        }
                    }

                }
                currentString.append("\n");
            }
            is.close();
        } catch (IOException e) {
            logger.error("IO Exception : File not found " + e);
        }
        return currentString.toString();
    }

    public long getBytesRead() {
        return bytesRead;
    }
}
