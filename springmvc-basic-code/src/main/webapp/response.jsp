<%--
  Created by IntelliJ IDEA.
  User: ChavinKing
  Date: 2020/1/11
  Time: 20:13
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" isELIgnored="false" %>
<html>
<head>
    <title>Title</title>

    <%-- idea环境如果运行失败，报错js/jquery-3.3.1.min.js不存在，请将js目录添加到idea out目录下 --%>
    <script src="js/jquery-3.3.1.min.js"></script>


    <%--<script src="https://code.jquery.com/jquery-3.4.1.min.js"></script>--%>

    <script>

        $(function(){
            $("#btn").click(function(){
                // alert("hello btn");
                // 发送ajax请求
                $.ajax({
                    // 编写json格式，设置属性和值
                    url:"user/testAjax",
                    contentType:"application/json;charset=UTF-8",
                    data:'{"name":"pgman","age":"30","hiredate":"2020-01-11"}',
                    dataType:"json",
                    type:"post",
                    success:function(data){
                        // data服务器端响应的json的数据，进行解析
                        alert(data);
                        alert(data.name);
                        alert(data.age);
                        alert(data.hiredate);
                    }
                });

            });
        });

    </script>

</head>
<body>

<a href="user/testString" >testString</a>

<br>

<a href="user/testVoid" >testVoid</a>

<br>

<a href="user/testModelAndView" >testModelAndView</a>

<br/>

<a href="user/testForwardOrRedirect" >testForwardOrRedirect</a>

<br/>

<button id="btn">发送ajax的请求</button>


</body>
</html>
