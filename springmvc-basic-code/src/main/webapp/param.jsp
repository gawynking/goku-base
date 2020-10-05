<%--
  Created by IntelliJ IDEA.
  User: ChavinKing
  Date: 2020/1/7
  Time: 23:33
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>Title</title>
</head>
<body>

<a href="param/testParam?username=pgman&password=pgsql">请求参数</a>

<br>
<br>
<br>

<form action="/param/saveAccount" method="post">
    用户名：<input type="text" name="username"><br>
    密码：<input type="text" name="password"><br>
    金额：<input type="text" name="money"><br>
    用户姓名：<input type="text" name="user.name"><br>
    用户年龄：<input type="text" name="user.age"><br>

    <input type="submit" value="提交">
</form>


<br>
<br>
<br>

<form action="/param/saveAccount" method="post">
    用户名：<input type="text" name="username"><br>
    密码：<input type="text" name="password"><br>
    金额：<input type="text" name="money"><br>

    用户姓名：<input type="text" name="user.name"><br>
    用户年龄：<input type="text" name="user.age"><br>

    用户姓名：<input type="text" name="users[0].name"><br>
    用户年龄：<input type="text" name="users[0].age"><br>

    用户姓名：<input type="text" name="maps['one'].name"><br>
    用户年龄：<input type="text" name="maps['one'].age"><br>

    <input type="submit" value="提交">
</form>


<br>
<br>
<br>

<form action="/param/saveUser" method="post">
    用户名：<input type="text" name="name"><br>
    年龄：<input type="text" name="age"><br>
    生日：<input type="text" name="hiredate"><br>
    <input type="submit" value="提交">
</form>

<br>
<br>
<br>

<a href="param/testServlet">Servlet原生的API</a>

</body>
</html>
