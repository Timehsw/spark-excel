### 说明
1.spark-excel支持分布式读取Excel2003版和2007版  
2.默认取Excel文件的第一行作为结构  
3.如果不把第一行作为结构,那么默认取列名为C1,C2,C3.....  
4.如果不把第一行作为结构,那么用户可以给StructType  
5.支持读取指定sheet页,也可以读取所有sheet页  
6.支持指定分隔符  
7.支持类型推断  
8.如果不做类型推断那么默认就都是String类型  
9.支持指定时间类型的格式输出  

### 用法

filePath:String,//Excel文件路径  
sheetNum:String=ExcelOptions.DEFAULT_SHEET_NUMBER,//解析第几个sheet页  
isAllSheet:String=ExcelOptions.DEFAULT_ALL_SHEET,//是否解析所有的sheet页  
useHeader: Boolean = ExcelOptions.DEFAULT_USE_HEADER,//是否把第一行当作结构去解析  
delimiter: Char = ExcelOptions.DEFAULT_FIELD_DELIMITER.charAt(0),//默认分隔符  
mode: String = ExcelOptions.DEFAULT_PARSE_MODE,//解析方式  
charset: String = ExcelOptions.DEFAULT_CHARSET,// 字符编码  
inferSchema: Boolean = ExcelOptions.DEFAULT_INFERSCHEMA//是否进行类型推断  

# 前言

> 实现的目标:实现分布式的读取excel文件,并且注册成spark中的dataframe类型，借助于spark平台对excel文件做分析.

# 参考项目

[spark-xml](https://github.com/databricks/spark-xml)<br>
[spark-csv](https://github.com/databricks/spark-csv)

# 如何新建数据源？

通过通读以上databricks公司开源的数据源.知道了自定义数据源主要在于实现sparksql的相关接口即可.<br>
比如以下几个:

- 注册数据源接口(可选)
- RelationProvider
- SchemaRelationProvider
- CreatableRelationProvider
- 产生Relation接口(可选)
- BaseRelation
- TableScan
- PrunedScan
- InsertableRelation

# 新建数据源的步骤

## 1.写一个DefaultSource类

**注意:** 类名必须叫DefaultSource.源码里解释了,sparksql会把DefaultSource这个名字加到路径中去搜索.我亲测如果取别的名字确实会报找到不到的错误. 这个DefaultSource类需要继承这几个抽象类，实现它们的方法:

**RelationProvider**

```
trait RelationProvider {
  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}
```

**SchemaRelationProvider**

```
trait SchemaRelationProvider {
  /**
   * Returns a new base relation with the given parameters and user defined schema.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation{

        ???
  /**
  * 重要的是这里了.我们继承的方法叫createRelation.因此就是要创建一个relation
  * 这个relation单例类.需要我们先去创建.继承他的父类BaseRelation,实现相关方法即可.
  * 然后在这里去调用这个方法就好了
  *
  * 值得注意的是第一个参数.惰性求值.需要用到的时候我们才求值
  * 第一个参数需要的是一个RDD.我们需要有一个类把我们的Excel数据读到分布式的RDD中
  *   因此我们重写了一个ExcelImportFormat
  * 剩下的参数,都是为了给这个普通的RDD赋予schema.最后成为一个DataFrame
  */
ExcelRelation(
  () => ExcelFile.withCharset(sqlContext.sparkContext, path,parameters, charset),
  Some(path),
  headerFlag,
  delimiter,
  parseMode,
  treatEmptyValuesAsNullsFlag,
  schema,
  inferSchemaFlag,
  codec,
  nullValue,
  dateFormat)(sqlContext)
      }
}
```

**CreatableRelationProvider**

```
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation
}
```

**注意:**<br>
1.这些类是可选的,根据自己需要的功能去借继承实现相应的类,那么后面你自定义的数据源也就有相应的功能了.这些类具体的功能可以看源码中的解释,非常详细,英文不好,就不在这里阐述了.<br>
2.故名思意,我们看这些类的命名和它们的抽象方法也能知道,这些类主要是根据提供的参数去产生一个Relation类.<br>
3.接下来说如何写一个Relation类

## 2.写一个ExcelRelation类

**注意:**要写一个ExcelRelation类需要继承几个类(可选)<br>
**BaseRelation**<br>
继承这个类是必须的,因为你需要给RDD提供schema

```
def schema: StructType
```

**TableScan** 这个类的buildScan方法无参,说明把RDD里面的所有数据都加载进来

```
trait TableScan {
  def buildScan(): RDD[Row]
}
```

**PrunedScan** 这个有参的buildScan是说明在生成RDD之前消除你不需要的列

```
trait PrunedScan {
def buildScan(requiredColumns: Array[String]): RDD[Row]
}
```

**InsertableRelation** 这个是关于插入的方法的

```
trait InsertableRelation {
  def insert(data: DataFrame, overwrite: Boolean): Unit
}
```

**注意:**

```
case class ExcelRelation protected[excel](
   baseRDD: () => RDD[String],//这里很重要.意味着我们首先需要有一个RDD.
   location: Option[String],
   useHeader: Boolean,
   delimiter: Char,
   parseMode: String,
   treatEmptyValuesAsNulls: Boolean,
   userSchema: StructType = null,
   inferExcelSchema: Boolean,
   codec: String = null,
   nullValue: String = "",
   dateFormat: String = null)(@transient val sqlContext:SQLContext)
  extends BaseRelation
  with InsertableRelation
  with TableScan //把所有的Row对象全部包括到RDD中
  with PrunedScan
```

说明:<br>
protected[spark] ExcelRelation只在spark包下被访问.ExcelRelation 是case class 意味着使用它的时候可以不需要new ,并且case class 可以带参数，但是object是不可以带参数的.<br>
() => 这是非严格求值的写法,惰性求值。<br>
那么问题来了:这个RDD是怎么来的.如何把Excel中的数据加载到分布式的RDD中呢?

### 3.把Excel中的数据加载到分布式的RDD中

> () => ExcelFile.withCharset(sqlContext.sparkContext, path,parameters)

这句代码之前在DefaultSource中就出现过.并且是作为第一个参数.<br>
ExcelFile.withCharset这个方法返回的就是一个RDD,那么我们就来看看这里面写的是啥

```
private[excel] object ExcelFile {
  val DEFAULT_INDENT="  "
  val DEFAULT_ROW_SEPARATOR="\n"
  val DEFAULT_CHARSET=Charset.forName("UTF-8")

  def withCharset(
    context:SparkContext,
    location:String,
    parameters:Map[String,String],
    charset:String="utf-8"):RDD[String]={
    val options=ExcelOptions(parameters)
    context.hadoopConfiguration.set(ExcelInputFormat.ENCODING_KEY,charset)
    context.hadoopConfiguration.set(ExcelInputFormat.EXCEL_SHEET_NUMBER,options.sheetNumm)
    context.hadoopConfiguration.set(ExcelInputFormat.EXCEL_ALLSHEET,options.isAllSheet)
    context.hadoopConfiguration.set(ExcelInputFormat.DEFAULT_FIELD_DELIMITER,options.delimiter)

    if (Charset.forName(charset)==DEFAULT_CHARSET) {
      context.newAPIHadoopFile(location,
        classOf[ExcelInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength))
    } else {
      context.newAPIHadoopFile(location,
        classOf[ExcelInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
    }
  }
}
```

**注意:**

1. 关注参数中的SparkContext:<br>
  通过这个上下文,调用newAPIHadoopFile返回了一个RDD.
2. 关注参数Map:<br>
  这里map里面的参数是外面传进来的,目的是将这些参数通过hadoopConfiguration.set到hadoop的配置文件中,这样后面通过hadoopConfiguration就可以取到传进来的参数了.
3. 算子newAPIHadoopFile:<br>
  3.1 源码里面是这样解释的:对于一个Hadoop文件,可以通过任意一个或者新的InputFormat去得到一个RDD.并且额外的参数也可以传到输入格式中.(就通过我上面说的那种方式) 3.2 newAPIHadoopFile算子参数的解释:第一个参数是:hadoopfile的路径,第二个参数是你用哪一个InputFormat类去解析你的hadoopfile.第三个参数是Record的key值类型.第三个参数是Record的value值类型.

#### 3.1 自定义ExcelInputFormat

```
class ExcelInputFormat extends TextInputFormat {
  override def createRecordReader(
                   split: InputSplit,
                   context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new ExcelRecordReader()
  }
}
```

因为excel也是文本类型,因此我继承了TextInputFormat.它的

<k,v>就代表 &lt;行偏移,该行内容&gt;.
然而系统所提供的这几种固定的将InputFile转换为 <k,v>的方式有时候并不能满足我们的需求:<br>此时需要我们自定义<strong>InputFormat</strong>,从而使Hadoop框架按照我们预设的方式来将InputFile解析为<k,v>
在领会自定义<strong>InputFormat</strong>之前，需要弄懂一下几个抽象类、接口及其之间的关系：</k,v></k,v></k,v>

```
InputFormat(interface), FileInputFormat(abstract class), TextInputFormat(class),
RecordReader (interface), Line RecordReader(class)的关系
      FileInputFormat implements  InputFormat
      TextInputFormat extends  FileInputFormat
      TextInputFormat.get RecordReader calls  Line RecordReader
      Line RecordReader  implements  RecordReader
```

对于InputFormat接口，上面已经有详细的描述 再看看 FileInputFormat，它实现了 InputFormat接口中的 getSplits方法，而将 getRecordReader与isSplitable留给具体类(如 TextInputFormat )实现， isSplitable方法通常不用修改，所以只需要在自定义的 InputFormat中实现 getRecordReader方法即可，而该方法的核心是调用 Line RecordReader(即由LineRecorderReader类来实现 " 将每个split解析成records, 再依次将record解析成

<k,v>对" )，该方法实现了接口RecordReader</k,v>

```
public abstract class RecordReader<KEYIN, VALUEIN> implements Closeable {

  public abstract void initialize(InputSplit split,
                                  TaskAttemptContext context
                                  ) throws IOException, InterruptedException;

  public abstract
  boolean nextKeyValue() throws IOException, InterruptedException;

  public abstract
  KEYIN getCurrentKey() throws IOException, InterruptedException;

  public abstract
  VALUEIN getCurrentValue() throws IOException, InterruptedException;

  public abstract float getProgress() throws IOException, InterruptedException;

  public abstract void close() throws IOException;
}
```

因此自定义InputFormat的核心是自定义一个实现接口RecordReader类似于LineRecordReader的类，该类的核心也正是重写接口RecordReader中的几大方法.<br>
我们的ExcelRecordReader如下所示,具体实现的上述几个方法的具体过程看后面的代码 private[excel] class ExcelRecordReader extends RecordReader[LongWritable, Text]

### 后记

整个流程大概就是这样了.其中具体的实现逻辑就要看代码了 [代码地址](https://github.com/Timehsw/spark-excel)

