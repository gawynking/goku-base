<%--
  Created by IntelliJ IDEA.
  User: ChavinKing
  Date: 2020/1/12
  Time: 17:55
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>Title</title>
</head>
<body>

<a href="account/testFindAll">测试查询所有</a>


<br>
<br>
<br>
<h3>测试保存账户</h3>

<form action="account/saveAccount" method="post">
    姓名：<input type="text" name="name" /><br/>
    金额：<input type="text" name="money" /><br/>
    <input type="submit" value="保存"/><br/>
</form>

</body>
</html>
