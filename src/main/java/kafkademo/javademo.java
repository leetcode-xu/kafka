package kafkademo;

import akka.dispatch.Foreach;
import scala.tools.nsc.backend.icode.Opcodes;
import scala.tools.nsc.doc.model.Public;
import scala.util.parsing.json.JSON;
import scala.xml.dtd.PublicID;

import java.lang.reflect.Array;
import java.security.PublicKey;
import java.sql.SQLOutput;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static scala.util.parsing.json.JSON.*;

class Demo<T extends Integer>{
    public T a;
    Demo(T a){
        this.a = a;
    }
    public void fun(T t){
        System.out.println(t);

        System.out.println();
        System.out.println(Arrays.toString((String[])Array.newInstance(String.class, 8)));
    }

    <T> T ceshi(T b){
        ArrayList<T> ts = new ArrayList<T>();
        ts.add(b);
        System.out.println(ts.get(0).getClass());


        System.out.println(((Integer)ts.get(0)).sum(1,2));
        return b;
    }
}

abstract class Xu{
    public void fun(){};
    public abstract void f();
}
interface Ceshi<T>{

    public static void fun(){
        System.out.println(0);
    }
}
class Student{
    public int age;
    public String sex;
    public String name;
    public Student(String name, String sex, int age){
        this.name = name;
        this.sex = sex;
        this.age = age;
    }
    public static int fun(int x){
        return x+1;
    }
    public void print(){
        System.out.println("woshistudent print");
    }
    public String toString(){
        return this.name + "-" + this.age + ':' + this.sex + "|";
    }

//    public int compareTo(Student s){
//        if (this.age>s.age){
//            return 1;
//        }
//        else{
//            return -1;
//        }
//    }
}

public class javademo {
    public static void fun(Demo<?> a){
        System.out.println(a);
        System.out.println(a.getClass());
    }
    public static <k> void ceshi(k m){
        System.out.println(m);
    }
    public static <T> void printMsg( T... args){
        System.out.println(args.getClass());
        for(T t : args){
            System.out.println("泛型测试"+"t is " + t);
        }
    }
    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();
        Student student1 = new Student("xujimao1", "nan", 25);
        Student student2 = new Student("xujimao2", "nan", 25);
        Student student3 = new Student("xujimao3", "nan", 26);
        Student student4 = new Student("xujimao4", "nan", 20);
        ArrayList<Student> students = new ArrayList<>();
        students.add(student1);
        students.add(student2);
        students.add(student3);
        students.add(student4);
        Student[] ss = students.toArray(new Student[0]);
        new Demo<Integer>(new Integer(8)).ceshi(new Integer(9));
        List<String> strings = Arrays.asList("H", "e", "l", "o", "W", "r", "d");
        List<Object> collect = strings.stream().flatMap(new Function<String, Stream<String>>() {
            @Override
            public Stream<String> apply(String s) {
                return Arrays.stream(s.split(""));
            }
        }).collect(Collectors.toList());
//        collect.contains()
        int[] ints = {1,2,3};
        Arrays.asList(ints).stream().map(x-> Arrays.stream(x).boxed().map(xxx->xxx+1)).forEach(System.out::println);
    }

}
