<%@ Page Language="C#" Inherits="Webproject.Default" %>
<!DOCTYPE html>
<html>
<head runat="server">
	<title>Default</title>
</head>
<body>
	<form id="form1" runat="server" action="http://comp410s18-fb-warmup-project.appspot.com/sign">
            <label for="say">What kind of data do you want?</label>
            <input name="key1">
            <input name="val1">
		    <asp:Button id="button1" runat="server" Text="submit" OnClick="button1Clicked" />
	</form>
</body>
</html>
