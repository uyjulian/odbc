#define NOMINMAX

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <algorithm>
#include <list>
#include <vector>

#include "ncbind/ncbind.hpp"
#include "tp_stub.h"

//----------------------------------------------------------------------
// Static Variables
//----------------------------------------------------------------------
static tjs_uint32 addHint, joinHint, getYearHint, getMonthHint, getDateHint, getHoursHint, getMinutesHint, getSecondsHint, countHint, shiftHint;

//----------------------------------------------------------------------
// Utility Functions
//----------------------------------------------------------------------
tTJSVariant createArray(void)
{
  iTJSDispatch2 *obj = TJSCreateArrayObject();
  tTJSVariant result(obj, obj);
  obj->Release();
  return result;
}

tjs_uint countArray(tTJSVariant array)
{
  static tjs_uint32 countHint;
  ncbPropAccessor arrayObj(array);
  return arrayObj.GetValue(L"count", ncbTypedefs::Tag<tjs_uint>(), 0, &countHint);
}

tTJSVariant createDictionary(void)
{
  iTJSDispatch2 *obj = TJSCreateDictionaryObject();
  tTJSVariant result(obj, obj);
  obj->Release();
  return result;
}

//----------------------------------------------------------------------
// Macros
//----------------------------------------------------------------------
#define TRYODBC(h, ht, x)                               \
  {   RETCODE rc = x;                                   \
    if (rc != SQL_SUCCESS)                              \
      {                                                 \
        dumpDiagnosticRecord (h, ht, rc);               \
      }                                                 \
    if (rc == SQL_ERROR)                                \
      {                                                 \
        WCHAR buf[1024];                                \
        swprintf_s(buf, 1023, L"Error in " TJS_W(#x) L"\n");  \
        TVPThrowExceptionMessage(buf);                  \
      }                                                 \
  }

//----------------------------------------------------------------------
// ODBC class
//----------------------------------------------------------------------
class ODBC
{
public:
  //----------------------------------------------------------------------
  enum QueryResultType  {
    qrtArray,
    qrtDictionary,
    qrtSingleColumnArray,
  };

private:
  //----------------------------------------------------------------------
  struct Binding {
    WCHAR               *buffer;  /* column buffer   */
    SQLLEN              ind;      /* size or null     */
    SQLLEN              type;     /* column type */
  };
  typedef std::list<Binding> bindings;
  
  //----------------------------------------------------------------------
  SQLHENV     mHEnv;
  SQLHDBC     mHDbc;
  SQLHSTMT    mHStmt;
  
  bool mIsConnected;
  bindings mBindings;

  //----------------------------------------------------------------------
  static void dumpDiagnosticRecord (SQLHANDLE      handle,    
                                    SQLSMALLINT    type,  
                                    RETCODE        retCode)
  {
    SQLSMALLINT rec = 0;
    SQLINTEGER  error;
    WCHAR       message[1000];
    WCHAR       state[SQL_SQLSTATE_SIZE+1];
    
    if (retCode == SQL_INVALID_HANDLE) {
      TVPAddLog(L"Invalid handle!");
      return;
    }
    
    while (SQLGetDiagRec(type,
                         handle,
                         ++rec,
                         state,
                         &error,
                         message,
                         (SQLSMALLINT)(sizeof(message) / sizeof(WCHAR)),
                         (SQLSMALLINT *)NULL) == SQL_SUCCESS)
      {
        // Hide data truncated..
        if (wcsncmp(state, L"01004", 5))
          {
            WCHAR buf[1024];
            swprintf_s(buf, 1023, L"[%5.5s] %s (%d)\n", state, message, error);
            TVPAddLog(buf);
          }
      }
  }

  //----------------------------------------------------------------------
  static void dumpAffectedRows(HANDLE hStmt) {
    SQLLEN rowCount;
    
    TRYODBC(hStmt,
            SQL_HANDLE_STMT,
            SQLRowCount(hStmt, &rowCount));
    
    if (rowCount >= 0)
      {
        WCHAR buf[1024];                                                
        swprintf_s(buf,
                   L"%Id %s affected\n",
                   rowCount,
                   rowCount == 1 ? L"row" : L"rows");
        TVPAddLog(buf);
      }
  }
  
public:
  //----------------------------------------------------------------------
  ODBC()
    : mHEnv(NULL)
    , mHDbc(NULL)
    , mHStmt(NULL)
    , mIsConnected(false)
  {
  }

  //----------------------------------------------------------------------
  ~ODBC() {
    disconnect();
    freeBindings();
  }

  //----------------------------------------------------------------------
  bool getConnected() {
    return mIsConnected;
  }
  
  //----------------------------------------------------------------------
  static tjs_error TJS_INTF_METHOD connect(tTJSVariant *result,
                                           tjs_int numparams,
                                           tTJSVariant **param,
                                           iTJSDispatch2 *objthis) {
    ODBC *self = ncbInstanceAdaptor<ODBC>::GetNativeInstance(objthis);
    if (! self) 
      return TJS_E_NATIVECLASSCRASH;
    ttstr connectStr;
    if (numparams == 0) connectStr = L"";
    else if (numparams == 1) connectStr = *param[0];
    else return TJS_E_BADPARAMCOUNT;
    ttstr resultConnectionStr = self->_connect(connectStr);
    if (result)
      *result = resultConnectionStr;
    return TJS_S_OK;
  }

  //----------------------------------------------------------------------
  ttstr _connect(ttstr connectStr) {
    disconnect();
    
    // Allocate an environment
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &mHEnv) == SQL_ERROR)
      TVPThrowExceptionMessage(L"Unable to allocate an environment handle");

    TRYODBC(mHEnv,
            SQL_HANDLE_ENV,
            SQLSetEnvAttr(mHEnv,
                          SQL_ATTR_ODBC_VERSION,
                          (SQLPOINTER)SQL_OV_ODBC3,
                          0));
    
    // Allocate a connection
    TRYODBC(mHEnv,
            SQL_HANDLE_ENV,
            SQLAllocHandle(SQL_HANDLE_DBC, mHEnv, &mHDbc));

    // Connect to the driver.
    WCHAR outConnectionStr[1024 + 1];
    SQLSMALLINT outConnectionStringLength = 0;
    RETCODE retCode;
    
    TRYODBC(mHDbc,
            SQL_HANDLE_DBC,
            retCode = SQLDriverConnect(mHDbc,
                                       GetDesktopWindow(),
                                       (SQLWCHAR*)connectStr.c_str(),
                                       SQL_NTS,
                                       outConnectionStr,
                                       1024 + 1,
                                       &outConnectionStringLength,
                                       SQL_DRIVER_COMPLETE_REQUIRED));
    
    if (retCode == SQL_NO_DATA)
      return L"";
        
    ttstr result;
    if (outConnectionStringLength <= 1024)
      result = outConnectionStr;
    
    TRYODBC(mHDbc,
            SQL_HANDLE_DBC,
            SQLAllocHandle(SQL_HANDLE_STMT, mHDbc, &mHStmt));

    mIsConnected = true;

    return result;
  }

  //----------------------------------------------------------------------
  void disconnect(void) {
    if (mHStmt)
      {
        SQLFreeHandle(SQL_HANDLE_STMT, mHStmt);
        mHStmt = NULL;
      }

    if (mHDbc)
      {
        SQLDisconnect(mHDbc);
        SQLFreeHandle(SQL_HANDLE_DBC, mHDbc);
        mHDbc = NULL;
      }

    if (mHEnv)
      {
        SQLFreeHandle(SQL_HANDLE_ENV, mHEnv);
        mHEnv = NULL;
      }

    mIsConnected = false;
  }
  
  //----------------------------------------------------------------------
  static tjs_error TJS_INTF_METHOD query(tTJSVariant *result,
                                         tjs_int numparams,
                                         tTJSVariant **param,
                                         iTJSDispatch2 *objthis) {
    ODBC *self = ncbInstanceAdaptor<ODBC>::GetNativeInstance(objthis);
    if (! self) 
      return TJS_E_NATIVECLASSCRASH;
    if (numparams == 0)
      return TJS_E_BADPARAMCOUNT;
    ttstr sqlStr;
    sqlStr = *param[0];
    QueryResultType queryResultType = qrtArray;
    if (numparams >= 2)
      queryResultType = QueryResultType(tjs_int(*param[1]));
    tTJSVariant queryResult;
    queryResult = self->_query(sqlStr, queryResultType);
    if (result)
      *result = queryResult;
    return TJS_S_OK;
  }

  //----------------------------------------------------------------------
  tTJSVariant _query(ttstr sqlString, QueryResultType queryResultType) {
    if (! mIsConnected) 
      TVPThrowExceptionMessage(L"SQL connection is not established.");

    RETCODE     retCode;
    SQLSMALLINT numResults;

    retCode = SQLExecDirect(mHStmt,(SQLWCHAR*)sqlString.c_str(), SQL_NTS);
    tTJSVariant result;
    
    switch(retCode)
      {
      case SQL_SUCCESS_WITH_INFO:
        {
          dumpDiagnosticRecord(mHStmt, SQL_HANDLE_STMT, retCode);
          // fall through
        }
      case SQL_SUCCESS:
        {
          // If this is a row-returning query, display
          // results
          TRYODBC(mHStmt,
                  SQL_HANDLE_STMT,
                  SQLNumResultCols(mHStmt,&numResults));
          
          if (numResults > 0) {
            result = fetchResults(numResults);
            switch (queryResultType) {
            case qrtArray: break;
            case qrtDictionary: result = convertQueryResultToDictionary(result); break;
            case qrtSingleColumnArray: result = convertQueryResultToSingleColumnArray(result); break;
            }
          }
          else
            dumpAffectedRows(mHStmt);
          break;
        }
        
      case SQL_ERROR:
        {
          dumpDiagnosticRecord(mHStmt, SQL_HANDLE_STMT, retCode);
          break;
        }
        
      default: {
        WCHAR buf[1024];                                                
        swprintf_s(buf, 1023, L"Unexpected return code %hd!", retCode);
        TVPThrowExceptionMessage(buf);
      }
      }

    return result;
  }

  //----------------------------------------------------------------------
  tTJSVariant fetchResults(SQLSMALLINT cCols) {
    tTJSVariant result;
    result = createArray();
    ncbPropAccessor resultObj(result);
    
    allocateBindings(cCols);
    tTJSVariant titles = fetchTitles();
    resultObj.FuncCall(0, L"add", &addHint, NULL, titles);

    RETCODE         RetCode = SQL_SUCCESS;
    bool fNoData = false;
    
    do {
      tTJSVariant columns = fetchColumns();
      if (columns.Type() == tvtVoid)
        fNoData = true;
      else 
        resultObj.FuncCall(0, L"add", &addHint, NULL, columns);
    } while (! fNoData);
        
    freeBindings();
    
    return result;
  }

  //----------------------------------------------------------------------
  void allocateBindings(SQLSMALLINT   cCols) {
    freeBindings();
    
    SQLSMALLINT     col;
    SQLLEN          columnLength;
    
    for (col = 1; col <= cCols; col++) {
      Binding binding = {};

      // get column type
      TRYODBC(mHStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(mHStmt,
                              col,
                              SQL_DESC_CONCISE_TYPE,
                              NULL,
                              0,
                              NULL,
                              &binding.type));
      
      // get column length 
      TRYODBC(mHStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(mHStmt,
                              col,
                              SQL_DESC_LENGTH,
                              NULL,
                              0,
                              NULL,
                              &columnLength));

      // allocate column buffer.
      binding.buffer = (WCHAR *)malloc((columnLength+1) * sizeof(WCHAR));

      // bind buffer to column
      TRYODBC(mHStmt,
              SQL_HANDLE_STMT,
              SQLBindCol(mHStmt,
                         col,
                         SQL_C_TCHAR,
                         (SQLPOINTER) binding.buffer,
                         (columnLength + 1) * sizeof(WCHAR),
                         &binding.ind));

      mBindings.push_back(binding);
    }
  }

  //----------------------------------------------------------------------
  void freeBindings() {
    for (bindings::iterator iBinding = mBindings.begin();
         iBinding != mBindings.end();
         iBinding++) 
      free(iBinding->buffer);

    mBindings.clear();
  }

  //----------------------------------------------------------------------
  tTJSVariant  fetchTitles() {
    tTJSVariant result = createArray();
    ncbPropAccessor resultObj(result);

    SQLSMALLINT col, colBegin, colEnd;
    SQLSMALLINT titleLength;
      
    titleLength = 0;
    colBegin = 1;
    colEnd = SQLSMALLINT(mBindings.size() + 1);

    for (col = colBegin; col < colEnd; col++) {
      SQLSMALLINT columnNameLength;
      TRYODBC(mHStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(mHStmt,
                              col,
                              SQL_DESC_NAME,
                              NULL,
                              0,
                              &columnNameLength,
                              NULL));
      titleLength = std::max(titleLength, SQLSMALLINT(columnNameLength / 2));
    }

    WCHAR *wszTitle = new WCHAR[titleLength + 1];
    
    for (col = colBegin; col < colEnd; col++) {
      TRYODBC(mHStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(mHStmt,
                              col,
                              SQL_DESC_NAME,
                              wszTitle,
                              (titleLength + 1) * sizeof(WCHAR),
                              NULL,
                              NULL));
      
      resultObj.FuncCall(0, L"add", &addHint, NULL, ttstr(wszTitle));
    }
    delete[] wszTitle;
    
    return result;
  }

  //----------------------------------------------------------------------
  tTJSVariant fetchColumns() {
    RETCODE         RetCode = SQL_SUCCESS;
    tTJSVariant result;

    TRYODBC(mHStmt, SQL_HANDLE_STMT, RetCode = SQLFetch(mHStmt));
    
    if (RetCode == SQL_NO_DATA_FOUND)
      return result;

    result = createArray();
    ncbPropAccessor resultObj(result);

    for (bindings::iterator iBinding = mBindings.begin();
         iBinding != mBindings.end();
         iBinding++) {
      if (iBinding->ind == SQL_NULL_DATA) {
        resultObj.FuncCall(0, L"add", &addHint, NULL, tTJSVariant());
      } else {          
        tTJSVariant columnValue = ttstr(iBinding->buffer);
        switch (iBinding->type) {
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_TINYINT:
        case SQL_BIGINT:
          columnValue.ToInteger();
          break;
        case SQL_DECIMAL:
        case SQL_NUMERIC:
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
          columnValue.ToReal();
          break;
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_TYPE_TIMESTAMP: {
          ttstr exp = L"new Date('" + ttstr(columnValue) + "')";
          TVPExecuteExpression(exp, &columnValue);
          break;
        }
        }
        resultObj.FuncCall(0, L"add", &addHint, NULL, columnValue);
      }
    }
    
    return result;
  }

  //----------------------------------------------------------------------
  tTJSVariant convertQueryResultToDictionary(tTJSVariant table) {
    ncbPropAccessor tableObj(table);

    tTJSVariant titleRow;
    tableObj.FuncCall(0, L"shift", &shiftHint, &titleRow);
    ncbPropAccessor titleRowObj(titleRow);

    tjs_int colCount = countArray(titleRow);
    tjs_int rowCount = countArray(table);

    std::vector<ttstr> titles;
    for (tjs_int i = 0; i < colCount; i++)
      titles.push_back(titleRowObj.GetValue(i, ncbTypedefs::Tag<ttstr>()));

    tTJSVariant result = createArray();
    ncbPropAccessor resultObj(result);
    
    for (tjs_int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      tTJSVariant row;
      row = tableObj.GetValue(rowIndex, ncbTypedefs::Tag<tTJSVariant>());
      ncbPropAccessor rowObj(row);
      tTJSVariant rowDict = createDictionary();
      ncbPropAccessor rowDictObj(rowDict);
      for (tjs_int colIndex = 0; colIndex < colCount; colIndex++) {
        tTJSVariant col = rowObj.GetValue(colIndex, ncbTypedefs::Tag<tTJSVariant>());
        rowDictObj.SetValue(titles[colIndex].c_str(), col);
      }
      resultObj.FuncCall(0, L"add", &addHint, NULL, rowDict);
    }
    return result;
  }

  //----------------------------------------------------------------------
  tTJSVariant convertQueryResultToSingleColumnArray(tTJSVariant table) {
    ncbPropAccessor tableObj(table);
    tableObj.FuncCall(0, L"shift", &shiftHint, NULL);

    tjs_int rowCount = countArray(table);
    
    tTJSVariant result = createArray();
    ncbPropAccessor resultObj(result);

    for (tjs_int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      tTJSVariant row;
      row = tableObj.GetValue(rowIndex, ncbTypedefs::Tag<tTJSVariant>(), 0);
      ncbPropAccessor rowObj(row);
      tTJSVariant colValue = rowObj.GetValue(tjs_int(0), ncbTypedefs::Tag<tTJSVariant>());
      resultObj.FuncCall(0, L"add", &addHint, NULL, colValue);
    }
    return result;
  }
  
  //----------------------------------------------------------------------
  static tjs_error TJS_INTF_METHOD escapeString(tTJSVariant *result,
                                                tjs_int numparams,
                                                tTJSVariant **param,
                                                iTJSDispatch2 *objthis) {
    if (numparams == 0) return TJS_E_BADPARAMCOUNT;
    ttstr str = *param[0];
    ttstr func, delimiter = L" + ";
    if (numparams >= 2)
      func = *param[1];
    if (numparams >= 3)
      func = *param[2];
    ttstr resultString = _escapeString(str, func, delimiter);
    if (result)
      *result = resultString;
    return TJS_S_OK;
  }
    
  //----------------------------------------------------------------------
  static ttstr _escapeString(ttstr str, ttstr func, ttstr delimiter) {
    tTJSVariant resultArray = createArray();
    ncbPropAccessor resultArrayObj(resultArray);
    ttstr buf;

    for (const tjs_char *s = str.c_str();
         *s;
         s++) {
      if ((*s) < 0x20
          || (*s) == L'\'') {
        if (! buf.IsEmpty()) {
          resultArrayObj.FuncCall(0, L"add", &addHint, NULL, L"'" + buf + L"'");
          buf = L"";
        }
        resultArrayObj.FuncCall(0, L"add", &addHint, NULL, L"CHAR(" + ttstr(tjs_int(*s)) + L")");
      } else {
        buf += *s;
      }
    }
    if (! buf.IsEmpty()) 
      resultArrayObj.FuncCall(0, L"add", &addHint, NULL, L"'" + buf + L"'");

    tTJSVariant result;
    resultArrayObj.FuncCall(0, L"join", &joinHint, &result, delimiter);

    return func + ttstr(L"(") + ttstr(result) + ttstr(L")");
  }

  //----------------------------------------------------------------------
  static ttstr escapeStringAccess(ttstr str) {
    return _escapeString(str, L"", L" + ");
  }

  //----------------------------------------------------------------------
  static ttstr escapeStringSQLServer(ttstr str) {
    return _escapeString(str, L"", L" + ");
  }

  //----------------------------------------------------------------------
  static ttstr escapeStringOracle(ttstr str) {
    return _escapeString(str, L"CONCAT", L", ");
  }

  //----------------------------------------------------------------------
  static ttstr escapeStringMySQL(ttstr str) {
    return _escapeString(str, L"CONCAT", L", ");
  }

  //----------------------------------------------------------------------
  static ttstr escapeStringPostgreSQL(ttstr str) {
    return _escapeString(str, L"", L" || ");
  }

  //----------------------------------------------------------------------
  static ttstr encodeDate(tTJSVariant _date) {
    ncbPropAccessor dateObj(_date);
    tTJSVariant year, month, date, hours, minutes, seconds;
    dateObj.FuncCall(0, L"getYear", &getYearHint, &year);
    dateObj.FuncCall(0, L"getMonth", &getMonthHint, &month);
    dateObj.FuncCall(0, L"getDate", &getDateHint, &date);
    dateObj.FuncCall(0, L"getHours", &getHoursHint, &hours);
    dateObj.FuncCall(0, L"getMinutes", &getMinutesHint, &minutes);
    dateObj.FuncCall(0, L"getSeconds", &getSecondsHint, &seconds);
    WCHAR buf[1024];
    swprintf_s(buf,
               1024,
               L"'%04d-%02d-%02d %02d:%02d:%02d'",
               tjs_int(year),
               tjs_int(month) + 1,
               tjs_int(date),
               tjs_int(hours),
               tjs_int(minutes),
               tjs_int(seconds));
    return ttstr(buf);
  }
};

NCB_REGISTER_CLASS(ODBC)
{
  Constructor();

  Variant("qrtArray", int(ODBC::qrtArray));
  Variant("qrtDictionary", int(ODBC::qrtDictionary));
  Variant("qrtSingleColumnArray", int(ODBC::qrtSingleColumnArray));
  
  NCB_PROPERTY_RO(connected, getConnected);
  NCB_METHOD_RAW_CALLBACK(connect, ODBC::connect, 0);
  NCB_METHOD(disconnect);
  NCB_METHOD_RAW_CALLBACK(query, ODBC::query, 0);
  
  NCB_METHOD_RAW_CALLBACK(escapeString, ODBC::escapeString, 0);
  NCB_METHOD(escapeStringAccess);
  NCB_METHOD(escapeStringSQLServer);
  NCB_METHOD(escapeStringOracle);
  NCB_METHOD(escapeStringMySQL);
  NCB_METHOD(escapeStringPostgreSQL);

  NCB_METHOD(encodeDate);
};

