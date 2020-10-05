<%--
  Created by IntelliJ IDEA.
  User: ChavinKing
  Date: 2020/1/7
  Time: 17:33
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" isELIgnored="false" %>
<html>
<head>
    <title>Title</title>
</head>
<body>
<h3>执行成功</h3>

<%--${requestScope}--%>
<%--${sessionScope}--%>

<% System.out.println("success.jsp执行了");%>


${user.name}
${user.age}



</body>
</html>
