Java DBF reader and writer.

To add FastDBF4j dependency in your project using maven, add this to the pom.xml file (inside the project tag):

	<!-- FastDBF4j dependency -->
	<dependencies>
		<dependency>
			<groupId>com.socialexplorer</groupId>
			<artifactId>fastdbf4j</artifactId>
			<version>1.0.2</version>
		</dependency>
	</dependencies>

	<!-- SocialExplorer maven repository -->
    <repositories>
        <repository>
            <id>socialexplorer</id>
            <url>https://github.com/SocialExplorer/fastDBF4j/raw/master/maven-repo/</url>
        </repository>
    </repositories>