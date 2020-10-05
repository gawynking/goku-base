<%--
  Created by IntelliJ IDEA.
  User: ChavinKing
  Date: 2020/1/12
  Time: 18:02
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" isELIgnored="false" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<html>
<head>
    <title>Title</title>
</head>
<body>

<h3>查询所有账户</h3>

<c:forEach items="${accounts}" var="account">
    ${account.name}
    ${account.money}
    <br>
</c:forEach>

</body>
</html>
