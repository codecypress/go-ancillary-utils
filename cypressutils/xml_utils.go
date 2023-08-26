package cypressutils

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/antchfx/xmlquery"
	cErrors "github.com/pkg/errors"
	"os"
	"strconv"
	"strings"
)

func ParseXMLString(strXML string) (*xmlquery.Node, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()
	return xmlquery.Parse(strings.NewReader(strXML))
}

func ParseXMLFile(param interface{}) (*xmlquery.Node, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	switch param.(type) {
	case *os.File:
		{
			file := param.(*os.File)
			defer file.Close()
			doc, err := xmlquery.Parse(file)
			if err != nil {
				ThrowException(cErrors.Cause(err))
				return nil, err
			}

			return doc, nil
		}
	case string:
		{
			strFilePath := param.(string)

			reader, err := os.Open(strFilePath)

			if err != nil {
				ThrowException(cErrors.Cause(err))
				return nil, err
			}

			defer reader.Close()

			doc, err := xmlquery.Parse(reader)

			if err != nil {
				ThrowException(cErrors.Cause(err))
				return nil, err
			}

			return doc, nil
		}
	default:
		{
			err := cErrors.New(fmt.Sprintf("Unknown type %T. Acceptable arguments [File Pointer or Filepath]", param))
			ThrowException(err)
			return nil, err
		}
	}
}

func GetNode(doc *xmlquery.Node, strXPath string) (node *xmlquery.Node, err error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return nil, err
	}

	return xmlquery.FindOne(doc, strXPath), nil
}

func GetNodesList(doc *xmlquery.Node, strXPath string) (node []*xmlquery.Node, err error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return nil, err
	}

	return xmlquery.Find(doc, strXPath), nil
}

func GetTagValue(doc *xmlquery.Node, strXPath string) (value string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return "", err
	}

	node, err := GetNode(doc, strXPath)
	if err != nil || node == nil {
		return "", err
	}

	return node.InnerText(), nil
}

func ReadInt(doc *xmlquery.Node, strXpath string) (int, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return 0, err
	}

	strResult, err := GetTagValue(doc, strXpath)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(strResult)
}

func ReadBool(doc *xmlquery.Node, strXpath string) (bool, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return false, err
	}

	strResult, err := GetTagValue(doc, strXpath)
	if err != nil {
		return false, err
	}

	return strconv.ParseBool(strings.ToLower(strResult))
}

func ReadFloat32(doc *xmlquery.Node, strXpath string) (float64, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return 0, err
	}

	strResult, err := GetTagValue(doc, strXpath)
	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(strResult, 32)
}

func ReadFloat64(doc *xmlquery.Node, strXpath string) (float64, error) {

	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return 0, err
	}

	strResult, err := GetTagValue(doc, strXpath)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(strResult, 64)
}

func UpdateXMLTag(doc *xmlquery.Node, strXPath string, value interface{}) (string, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return "", err
	}

	tempVal := fmt.Sprintf("%v", value)

	node, err := GetNode(doc, strXPath)
	if err != nil || node == nil {
		return "", err
	}

	switch node.Type {
	case xmlquery.DocumentNode, xmlquery.ElementNode:
		{
			if node.FirstChild == nil {
				newTextNode := &xmlquery.Node{
					Data: tempVal,
					Type: xmlquery.TextNode,
				}
				node.FirstChild = newTextNode
			} else {
				node.FirstChild.Data = tempVal
			}
			break
		}
	case xmlquery.AttributeNode:
		{
			AddOrUpdateAttr(node.Parent, node.Data, tempVal)
			break
		}
	}

	return ConvertXMLToString(doc, false), nil
}

func UpdateXMLTags(doc *xmlquery.Node, mapXPathsData map[string]interface{}) (string, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return "", err
	}

	for xPath, value := range mapXPathsData {
		tempVal := fmt.Sprintf("%v", value)

		node, err := GetNode(doc, xPath)
		if err != nil || node == nil {
			return "", err
		}

		switch node.Type {
		case xmlquery.DocumentNode, xmlquery.ElementNode:
			{
				if node.FirstChild == nil {
					newTextNode := &xmlquery.Node{
						Data: tempVal,
						Type: xmlquery.TextNode,
					}
					node.FirstChild = newTextNode
				} else {
					node.FirstChild.Data = tempVal
				}

				break
			}
		case xmlquery.AttributeNode:
			{
				AddOrUpdateAttr(node.Parent, node.Data, tempVal)
				break
			}
		}
	}

	return ConvertXMLToString(doc, false), nil
}

func UpdateXMLTagsFromString(xmlString string, mapXPathsData map[string]interface{}) (string, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	doc, err := ParseXMLString(xmlString)
	if err != nil {
		return "", err
	}

	if doc == nil {
		err := cErrors.New("the XML Doc is NIL")
		ThrowException(err)
		return "", err
	}

	for xPath, value := range mapXPathsData {
		tempVal := fmt.Sprintf("%v", value)

		node, err := GetNode(doc, xPath)
		if err != nil || node == nil {
			return "", err
		}

		switch node.Type {
		case xmlquery.DocumentNode, xmlquery.ElementNode:
			{
				if node.FirstChild == nil {
					newTextNode := &xmlquery.Node{
						Data: tempVal,
						Type: xmlquery.TextNode,
					}
					node.FirstChild = newTextNode
				} else {
					node.FirstChild.Data = tempVal
				}
				break
			}
		case xmlquery.AttributeNode:
			{
				AddOrUpdateAttr(node.Parent, node.Data, tempVal)
				break
			}
		}
	}

	return ConvertXMLToString(doc, false), nil
}

func AddOrUpdateAttr(parent *xmlquery.Node, key string, value interface{}) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if parent == nil {
		err := cErrors.New("the XML Parent Node is NIL")
		ThrowException(err)
		return
	}

	tempVal := fmt.Sprintf("%v", value)

	var local, space string
	local = key
	if i := strings.Index(key, ":"); i > 0 {
		space = key[:i]
		local = key[i+1:]
	}

	var exists = false

	for i := 0; i < len(parent.Attr); i++ {
		attr := &parent.Attr[i]

		if attr.Name.Local == local && attr.Name.Space == space {
			attr.Value = tempVal
			exists = true
			break
		}
	}

	if !exists {
		xmlquery.AddAttr(parent, key, tempVal)
	}
}

func ReplaceNode(currentNode *xmlquery.Node, newNode *xmlquery.Node) {
	if currentNode.Parent == nil {
		return
	}

	if currentNode.Parent.FirstChild == currentNode {
		if currentNode.Parent.LastChild == currentNode {
			currentNode.Parent.FirstChild = newNode
			currentNode.Parent.LastChild = newNode
		} else {
			currentNode.Parent.FirstChild = newNode
			newNode.NextSibling = currentNode.NextSibling
			newNode.NextSibling.PrevSibling = newNode
		}
	} else {
		if currentNode.Parent.LastChild == currentNode {
			currentNode.Parent.LastChild = newNode
			newNode.PrevSibling = currentNode.PrevSibling
			newNode.PrevSibling.NextSibling = newNode
		} else {
			newNode.PrevSibling = currentNode.PrevSibling
			newNode.PrevSibling.NextSibling = newNode
			newNode.NextSibling = currentNode.NextSibling
			newNode.NextSibling.PrevSibling = newNode
		}
	}

	currentNode.Parent = nil
	currentNode.PrevSibling = nil
	currentNode.NextSibling = nil
	currentNode = nil

}

/*//Apparently I have to figure it out myself coz docs are as useless as the docs themselves
func CreateChildTextNode(tempVal string) (node *xmlquery.Node) {
	return &xmlquery.Node{
		Type: xmlquery.TextNode,
		Data: tempVal,
	}
}*/

func getAttrName(n *xmlquery.Node) xml.Name {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	key := n.Data

	if i := strings.Index(key, ":"); i > 0 {
		return xml.Name{Space: key[:i], Local: key[i+1:]}
	} else {
		return xml.Name{Local: key}
	}
}

func ConvertXMLToString(n *xmlquery.Node, self bool, indentAmountArr ...int) string {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	indentAmount := 4
	if indentAmountArr != nil {
		indentAmount = indentAmountArr[0]
	}

	var tabs strings.Builder
	for i := 0; i < indentAmount; i++ {
		fmt.Fprintf(&tabs, " ")
	}

	var buf bytes.Buffer
	if self {
		var indent = 0
		outputXML(&buf, n, &indent, tabs.String(), false)
	} else {

		for n := n.FirstChild; n != nil; n = n.NextSibling {
			if n.Data == "xml" {
				continue
			}
			var indent = 0
			outputXML(&buf, n, &indent, tabs.String(), false)
		}
	}

	return buf.String()
}

func calculatePreserveSpaces(n *xmlquery.Node, pastValue bool) bool {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if attr := n.SelectAttr("xml:space"); attr == "preserve" {
		return true
	} else if attr == "default" {
		return false
	}
	return pastValue
}

func sanitizedData(n *xmlquery.Node, preserveSpaces bool) string {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	if preserveSpaces {
		return strings.Trim(n.Data, "\n\t")
	}
	return strings.TrimSpace(n.Data)
}

func outputXML(buf *bytes.Buffer, n *xmlquery.Node, indentLevel *int, strIndentAmount string, preserveSpaces bool) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	var tabs strings.Builder
	for i := 0; i < *indentLevel; i++ {
		fmt.Fprintf(&tabs, strIndentAmount)
	}

	preserveSpaces = calculatePreserveSpaces(n, preserveSpaces)
	switch n.Type {
	case xmlquery.TextNode:
		xml.EscapeText(buf, []byte(sanitizedData(n, preserveSpaces)))
		*indentLevel = *indentLevel - 1
		return
	case xmlquery.CharDataNode:
		buf.WriteString(tabs.String())
		buf.WriteString("<![CDATA[")
		buf.WriteString(n.Data)
		buf.WriteString("]]>")
		buf.WriteString("\n")
		*indentLevel = *indentLevel - 1
		return
	case xmlquery.CommentNode:
		buf.WriteString(tabs.String())
		buf.WriteString("<!--")
		buf.WriteString(n.Data)
		buf.WriteString("-->")
		buf.WriteString("\n")
		*indentLevel = *indentLevel - 1
		return
	case xmlquery.DeclarationNode:
		buf.WriteString(tabs.String())
		buf.WriteString("<?" + n.Data)
	default:
		buf.WriteString(tabs.String())
		if n.Prefix == "" {
			buf.WriteString("<" + n.Data)
		} else {
			buf.WriteString("<" + n.Prefix + ":" + n.Data)
		}
	}

	for _, attr := range n.Attr {
		if attr.Name.Space != "" {
			buf.WriteString(fmt.Sprintf(` %s:%s=`, attr.Name.Space, attr.Name.Local))
		} else {
			buf.WriteString(fmt.Sprintf(` %s=`, attr.Name.Local))
		}
		buf.WriteByte('"')
		xml.EscapeText(buf, []byte(attr.Value))
		buf.WriteByte('"')
	}

	if n.Type == xmlquery.DeclarationNode {
		buf.WriteString("?>")
		buf.WriteString("\n")
	} else {
		var textChildren strings.Builder
		var allAreText = true
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if child.Type != xmlquery.TextNode && child.Type != xmlquery.CharDataNode {
				allAreText = false
				break
			} else {
				fmt.Fprintf(&textChildren, child.InnerText())
			}
		}

		var isEmptyTag = false
		if allAreText {
			if strings.Trim(textChildren.String(), "\n\t ") == "" {
				buf.WriteString("/>")
				buf.WriteString("\n")
				isEmptyTag = true
			} else {
				buf.WriteString(">")
			}
		} else {
			buf.WriteString(">")
			buf.WriteString("\n")
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			*indentLevel = *indentLevel + 1
			outputXML(buf, child, indentLevel, strIndentAmount, preserveSpaces)
		}

		var tabs strings.Builder
		for i := 0; i < *indentLevel; i++ {
			fmt.Fprintf(&tabs, strIndentAmount)
		}

		*indentLevel = *indentLevel - 1

		if !allAreText || !isEmptyTag {
			if !allAreText {
				buf.WriteString(tabs.String())
			}
			if n.Prefix == "" {
				buf.WriteString(fmt.Sprintf("</%s>", n.Data))
			} else {
				buf.WriteString(fmt.Sprintf("</%s:%s>", n.Prefix, n.Data))
			}
			buf.WriteString("\n")
		}
	}
}

/*func outputXML(buf *bytes.Buffer, n *xmlquery.Node, preserveSpaces bool) {
	var tabs strings.Builder
	for i := 0; i < n.GetLevel(); i++ {
		fmt.Fprintf(&tabs, " ")
	}

	preserveSpaces = calculatePreserveSpaces(n, preserveSpaces)
	switch n.Type {
	case xmlquery.TextNode:
		xml.EscapeText(buf, []byte(n.Data))
		return
	case xmlquery.CharDataNode:
		buf.WriteString(tabs.String())
		buf.WriteString("<![CDATA[")
		buf.WriteString(n.Data)
		buf.WriteString("]]>")
		//buf.WriteString("\n")
		return
	case xmlquery.CommentNode:
		buf.WriteString(tabs.String())
		buf.WriteString("<!--")
		buf.WriteString(n.Data)
		buf.WriteString("-->")
		//buf.WriteString("\n")
		return
	case xmlquery.DeclarationNode:
		buf.WriteString(tabs.String())
		buf.WriteString("<?" + n.Data)
	default:
		buf.WriteString(tabs.String())
		if n.Prefix == "" {
			buf.WriteString("<" + n.Data)
		} else {
			buf.WriteString("<" + n.Prefix + ":" + n.Data)
		}
	}

	for _, attr := range n.Attr {
		if attr.Name.Space != "" {
			buf.WriteString(fmt.Sprintf(` %s:%s=`, attr.Name.Space, attr.Name.Local))
		} else {
			buf.WriteString(fmt.Sprintf(` %s=`, attr.Name.Local))
		}
		buf.WriteByte('"')
		xml.EscapeText(buf, []byte(attr.Value))
		buf.WriteByte('"')
	}
	if n.Type == xmlquery.DeclarationNode {
		buf.WriteString("?>")
		//buf.WriteString("\n")
	}

	for i := 0; i < n.GetLevel(); i++ {

	}
	var textChildren strings.Builder
	var allAreText = true
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if child.Type != xmlquery.TextNode && child.Type != xmlquery.CharDataNode{
			allAreText = false
			break
		}else{
			fmt.Fprintf(&textChildren, child.InnerText())
		}
	}

	if allAreText {
		if strings.Trim(textChildren.String(),"\n\t ") == ""{
			buf.WriteString("/>")
			//buf.WriteString("\n")
		}else{
			buf.WriteString(">")
		}
	}else{
		buf.WriteString(">")
		//buf.WriteString("\n")
	}

	for child := n.FirstChild; child != nil; child = child.NextSibling {
		outputXML(buf, child, preserveSpaces)
	}

	if n.Type != xmlquery.DeclarationNode && !allAreText {
		if n.Prefix == "" {
			buf.WriteString(fmt.Sprintf("</%s>", n.Data))
		} else {
			buf.WriteString(fmt.Sprintf("</%s:%s>", n.Prefix, n.Data))
		}
	}
}*/

/*// OutputXML returns the text that including tags name.
func OutputXML(self bool) string {
	var buf bytes.Buffer




	if self {
		outputXML(&buf, n, false)
	} else {
		for n := n.FirstChild; n != nil; n = n.NextSibling {
			outputXML(&buf, n, false)
		}
	}

	return buf.String()
}*/
