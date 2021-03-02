# MapReduce项目——金庸的江湖

----------------------------------------------------------------------------------

报告内容包括程序设计的主要流程、程序采用的主要算法、进行的优化工作、优化取得的效果、程序的性能分析以及程序运行截图等

## 1. 实验目的

通过一个综合数据分析案例：“金庸的江湖——金庸武侠小说中的人物关系挖掘”，学习和掌握MapReduce程序设计，体会使用MapReduce完成一个综合性的数据挖掘任务的全流程：
- 数据预处理；
- 数据分析；
- 数据后处理；
- ……

熟悉和掌握以下MapReduce编程技能：
1. 在Hadoop中使用第三方的Jar包来辅助分析；

2. 掌握简单的MapReduce算法设计：
    -	单词同现算法；
    -	数据整理与归一化算法；
    -	数据排序；

3. 掌握带有迭代特性的MapReduce算法设计：
    -	PageRank算法；
    -	标签传播（Label Propagation）算法；


## 2. 实验过程

### 2.1 总体进度

| 任务 | 完成情况 |
|:----:|:-------:|
|人物关系预处理| √ |
|人物同现统计| √ |
|人物关系图构建与边权重归一化| √ |
|基于人物关系图的PageRank计算| √ |
|基于人物关系图的标签传播算法| √ |
|人物关系分析结果整理| √ |

### 2.2 流程分析

```flow 
st=>start: 开始 
e=>end:  结束

sub1=>subroutine: 对原始文本进行分词和人名提取 

sub2=>subroutine: 对提取出来的人名进行词同现统计

sub3=>subroutine: 对词同现统计结果做正则化

sub4=>subroutine: 计算图中节点的PageRank值，按照PR值排序输出

sub5=>subroutine: 对图做标签传播，按照相同标签在一起的原则输出

st->sub1->sub2->sub3->sub4->sub5->e 
```



###2.3 算法分析
####2.3.1 数据预处理：提取原始文段中的人物

本任务主要工作是从原始的金庸小说文本中提取出与人物互动相关的数据，屏蔽掉无关的内容，为后文的人物同现工作做好准备。
数据输入为全本的金庸小说集（没有分词），金庸小说的人名列表；
数据输出为分词后，仅保留人名的金庸武侠小说全集。

按照要求，我们需要对每一段进行分析，将本段中所有的人物抽取出来构建词同现关系，问题在于：有很多段只有一个人名，而这个人名又是和下一段的人进行沟通的，所以单纯的只按照段落进行分析不是很准确。

我们提出一个折衷的方法，首先设定一个人名数量阈值N，单个段落中的人物数量如果超过该阈值N，按照要求进行输出，如果没有达到这个阈值，接着看下一个自然段，直到人名超过N或者段落的数量超过最大限定阈值为止。
**GetNames.java**

**输入**: $lineNo, pureText$ 

**输出**: $BookName, namesInText$

**伪代码**：
```
class mapper: 
    void map(key,value):
        get the name of book where key comes from --> BookName;
        emit <BookName, value>

class reducer:
    set<String> allNames;
    void setup():
        add all the names to allNames and participle;
        
    void reducer(key, valueList):
        List Names;
        countLine = 0;
        for value in valueList:
            use the participle to deal with String:value;
            for each word in value:
                if this word is a name and is in set allNames:
                    add word to Names;
                if Names.size()>threshold1 or countLine>threshhold2:
                    emit <key, Names.allElements()>;       
                    countLine = 0;
                    Names.clear();
```     
#####部分结果：
```
射雕英雄传.txt    黄蓉 郭靖 
射雕英雄传.txt    洪七公 欧阳锋 杨康
射雕英雄传.txt    黄蓉 黄药师 梅超风
......
```

#### 2.3.2 特征提取：人物同现统计
**WordConcurrence.java**

**输入**: $Figure_0$, $Figure_1$, ... $Figure_N$

**输出**: <$Figure_i, Figure_j$> $count$;

**伪代码**:

```Java
class Mapper{
    void map(List<Figure> figures, Context context){
        for(Figure i : figures)
            for(Figure j : figures)
                context.emit(<i, j>, 1);
    }
}

class Reducer{
    void reduce(<Figure, Figure> pair, Iterable<Long> values, Context context){
        Long value = 0;
        for(Long each : values)
            value += each;
        context.emit(pair, value);
    }
}
```

##### 部分结果：

```
一灯大师,马钰	2

一灯大师,鲁有脚	4

一灯大师,黄药师	15

一灯大师,黄蓉	72

丁不三,丁不四	27
```

####2.3.3 人物关系图构建与特征归一化

**Normalization.java**

**输入**: <$Figure_i, Figure_j$> $count$;

**输出**: $A$ $PageRankValueNor$ | $B_0$ $weight_0$ $B_1$ $weight_1$ …… $B_N$ $weight_N$

**伪代码**:

```Java
class Mapper{
    void map(List<Figure> figures, Context context){
        for(Figure i : figures)
            persen1,person2,num = figures.splits("," ,"\t");
            context.emit(person0, <person1,num>);
    }
}

class Reducer{
    void reduce(Figure person0, Iterable<Text> values, Context context){
        Long sum = 0;
        StringBuilder output ;
        ArrayList<Integer>count;
        ArrayList<String>name;
        output.append("1|");//用于给pagerank初始化
        for(Text each : values)
            num=getNum(each);
            sum+=num;
            name.add(getName(each));
            count.add(num);
        for(String person:name){
            Double weight = count[person]/sum;
            output.append("%s %s ",person,weight);
        }
        context.emit(pair, value);
    }
}
```

##### 部分结果：

```
一灯大师	1.0|天竺僧 0.0023474179 完颜萍 0.0046948357 ...

丁不三	1.0|小翠 0.006134969 李万山 0.006134969 封万里 0.024539877 柯万钧 0.006134969 ... 

丁不四	1.0|龙岛主 0.01655629 大汉 0.009933775 天虚 0.0033112583 封万里 0.01655629 ... 

丁典	1.0|凌退思 0.05172414 万圭 0.047413792 万震山 0.01724138 农夫 0.004310345 ...

```

#### 2.3.4 数据分析：基于人物关系图的PageRank计算

**PageRank.java**

> 由于采用迭代的方式, 输入输出一致, 修改了任务3的输出简化**GraphBuilder**步骤;


**输入输出**: $A$ $PageRankValue$ | $B_0$ $weight_0$ $B_1$ $weight_1$ …… $B_N$ $weight_N$


**伪代码**:

```Java
public class PageRankIter{
    class Mapper{
        void map(Figure a, Node N, Context context){
            context.emit(a, N);
            for(Figure each : N.AdjacencyList){
                // 计算邻接节点的PageRank值并发射;
                context.emit(each, each.weight * a.PageRankValue);
            }
        }
    }

    class Reducer{
        void reduce(Figure a, Iterable<Node> values, Context context){
            value = null;
            for(Long each : values){
                if(isNode(each))
                    value.Node = each;
                else
                    value.PageRank += each;
            }
            // 随机浏览模型
            value.PageRank = value.PageRank * damping + (1-damping)*PageRankInitialValue;
            context.emit(pair, value);
        }
    }
}

public class PageRankDriver{
    public static void main(String[] args) throws Exception{
        int iteration = 0;
        String pgIN = args[0] + i;
        ++i;
        String pgOut = args[1] + i;
        for(;iteration < PageRank.IterationTimes; ++iteration){
            PageRankIter.main(new String[]{pgIn, pgOut});
            pgIN = pgOut;
            pgOut = args[1] + i;
        }
    }
}
```
###2.3.5 数据分析：在人物关系图上的标签传播

**迭代部分输入输出**: $A$ $label$ & $B_0$,$weight_0$,$B_0 label$; $B_1$,$weight_1$,$B_1 label$; …… $B_N$,$weight_N$,$B_N label$;
**最终输出**：$A$ $labelInt$ $label$

**伪代码**:

```Java
public class LPA{
    class Mapper{
        void map(Figure a, Text N, Context context){
            List<name,weight,label> table;
            person,table=N.split();//得到所有相邻节点、对应的权重和对应的标签
            HashMap<label,weight> map;
            for(<name,weight,label> :table){
                if(label in map)
                    map[label]+=weight;
                else
                    map[label]=weight;//得到所有临近节点的标签和对应的权重
            }
            getMaxWeightLabel(map);//得到最大权重的标签作为person的标签
            context.emit(person, label+table);//用于维持图结构
            for(name : table){
                context.emit(name,<person,label>);//用于在reduce节点中更新<name,weight,label>数组中的label
            }
        }
    }

    class Reducer{
        void reduce(Text person, Iterable<Text> values, Context context){
            HashMap<person,label> map;
            for(value:values){
                if(isPersonToLabelPair(value)){
                    map.put(value.person,value.label);
                }
                else{
                    person,label，table=value.split();
                    for(<name,weight,label> :table)
                        lable=map[name];//更新邻接表每个人的lable
                    context.emit(person, label+table);   
                }
        }
    }

    class cleanupMapper{//最后处理输出，将邻接表部分去除，只留下人物标签对，便于作图处理
        void map(Figure a, Text N, Context context){
            List<name,weight,label> table;
            person,label,table=N.split();
            context.emit(label,person);//使得输出的时候按照label排序
        }
    }

    class cleanupReducer{
        void reduce(Text label, Iterable<Text> people, Context context){
            for(person:people)
                context.emit(person, label); //输出人物标签对
        }
    }

}

public class LPADriver{
    public static void main(String[] args) throws Exception{
        int iteration = 0;
        String pgIN = args[0] + i;
        String pgOut = args[1] + i;
        int MAX = integer(args[2]);
        for(;iteration < MAX-1; ++iteration){
            setMapper(Mapper);
            setMapper(Reducer);
            LPA.main(new String[]{pgIn, pgOut});
            pgIN = pgOut;
            pgOut = args[1] + i;
        }
        setMapper(cleanupMapper);
        setMapper(cleanupReducer);
        LPA.main(new String[]{pgIn, pgOut});
    }
}
```
##### 部分结果：

```

天门道人	1 令狐冲

冲虚道长	1 令狐冲

祖千秋	1 令狐冲

哑婆婆	1 令狐冲
```
#### 2.3.6 统计整理结果

任务6分为两个部分，第一部分是协助任务4的pagerank算法，将其输出按照pagerank值排序，这一部分的代码在SortPR.java中。

我们可以直接利用mapreduce程序自带的partition进行排序，我们直到，partition会自动整理map阶段的输出，按照输出的升序传送给reducer。如果我们仅设定一个reducer，在map阶段将key value交换，输出到reducer的必然是按照value升序排列的键值对。
但是我们期望的结果是逆序的，我们可以将value取负值（原始的pr值都为正）来解决。

SortPR.java伪代码如下:
```
class mapper:
    void map(key, value):
        emit <-value, key>;
class reducer:
    void reduce(key, valueList):
        for each in valueList:
            emit <each, -key>
```

第二部分是对标签传播算法进行整理，将拥有相同标签的人名整合在一起，依然采用交换key value的方法，相同的value就会传送给同一个reducer
GatherLabels.java的伪码表示为：
```
class Mapper:
     void map(key, value):
        emit(value, key);
class Reducer:
     void reduce(key, valueList):
        for each in valueList:
            emit(key, value);
```
这里不再展示实验结果，因为同任务5和任务4格式相同，仅仅交换了次序。


## 3. 实验结果


### 结果分析与展示

在实验输出的基础上，我们对词同现关系做了可视化，在接下来的图中，节点的大小表明其PageRank值的大小，节点的颜色表明了其位于的group（标签传播算法的标签），有较多联系的节点位置会相邻在一起，联系较少的节点会彼此远离：


我们可以看出，图基本上围绕着小说的主人公进行展开，且主人公一般占据着最显眼的位置（节点最大），不同小说之间由于联系较少分得比较开，如下图：

但是有的小说人物之间有紧密联系，其节点挨得比较近，如射雕英雄传和神雕侠侣：


### 优化与效果

####1. 从原始文本中获取同现的人名
正如在2.3.1的算法描述中所讲的，实验要求给出同一段中的所有出现的人名，但是可能出现下面的情况：
```
    张无忌道：“@#￥%……&……%￥%……&*（”
    周芷若道：”*&……&*&*&*&……&*&&*“
    小昭则言：“&*（*&*（*&*&*（*&*（&%￥”
```
这里包含了三个段落，每个段落只有一个人名，如果单纯按照段落做划分，只有一个名字的段落没有意义（没有体现出人物间的关系），应该被舍弃，但实际上这三个段落是一个整体，包含着主角之间的交流信息。某些作家，特别是古龙，非常喜欢用较短的分段来渲染氛围，因此单纯按照分段来做会损失不少有用信息。

为了克服这个缺点，我们设置了两个阈值N1和N2，一个自然段的人名如果大于等于N1，直接输出，如果小于N1，再往下看几个自然段，直到人名数量大于N1或者自然段数量大于N2为止。由于需要考虑到多个自然段的内容，我们把分词和人名的提取人物放到了reduce阶段，详情请查看上方2.3.1的伪代码。

####2.  PageRank终止条件

利用两种方式判断PageRank是否终止迭代:

1. 迭代固定次数

    设定最大迭代次数, 伪代码如下
    ```Java
    public class PageRank{
        ...
        public static void main(String[] args){
            ...
            do{
                ...
            } while(iteration < PageRank.MAX_ITERATION_TIME);
        }
    }
    ```

2. 各人物的PageRank值收敛

    利用**Hadoop**计数器功能对PageRank值变化的人物进行计数, 若不再变化则判断收敛, 停止迭代, 伪代码如下

    ```Java
    public class PageRank{
        public enum eInf{
            COUNTER
        }

        ...

        public static class PageRankReducer{
            ...
            public void reduce(){
                ...
                if(Math.abs(lastRankValue - pageRankValue) < 1e-6){
                    context.getCounter(eInf.COUNTER).increment(1L);
                }
                ...
            }
        }

        public static void main(String[] args){
            ...
            do{
                ...
                if(jobPR.getCounters().findCounter(PageRank.eInf.COUNTER).getValue() == 0)
                    break;
                ...
            } while(iteration < PageRank.MAX_ITERATION_TIME);
            ...
        }
    }
    ```


