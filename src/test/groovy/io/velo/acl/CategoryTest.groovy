package io.velo.acl

import spock.lang.Specification

class CategoryTest extends Specification {
    def 'test all'() {
        expect:
        Category.getCmdListByCategory(Category.dangerous).size() > 0
        Category.getCategoryListByCmd('acl').size() > 0
    }
}
