package groovy.com.cloud.ldap

import com.cloud.ldap.NoSuchLdapUserException
import spock.lang.Specification

class NoSuchLdapUserExceptionSpec extends Specification {
    def "Test that the username is correctly set within the No such LDAP user exception object"() {
        given: "You have created an No such LDAP user exception object with the username set"
        def exception = new NoSuchLdapUserException(username)
        expect: "The username is equal to the given data source"
        exception.getUsername() == username
        where: "The username is set to "
        username << ["", null, "rmurphy"]
    }
}
