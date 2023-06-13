//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

@discardableResult
public func letNotNil<T1, T2>(_ instance: T1?, _ operation: (T1) throws -> T2?) rethrows -> T2? {
    guard let instance = instance else { return nil }
    return try operation(instance)
}

@discardableResult
public func letNotNil<T1, T2>(_ instance: inout T1?, _ operation: (inout T1) throws -> T2?) rethrows -> T2? {
    guard var instance = instance else { return nil }
    return try operation(&instance)
}

@discardableResult
public func letNotNil<T1, T2>(_ instance: T1?, default: @autoclosure () -> T2, _ operation: (T1) throws -> T2) rethrows -> T2 {
    guard let instance = instance else { return `default`() }
    return try operation(instance)
}

internal extension Array where Element: Equatable {
    
    @discardableResult
    mutating func remove(_ element: Element) -> Bool {
        if let index = firstIndex(of: element) {
            remove(at: index)
            return true
        }
        return false
    }
    
}

internal extension Array {
    
    mutating func popFirst() -> Element? {
        if isEmpty {
            return nil
        } else {
            return removeFirst()
        }
    }
    
    mutating func removeFirst(where predicate: (Element) -> Bool) -> Element? {
        guard let index = firstIndex(where: predicate) else { return nil }
        return remove(at: index)
    }
    
}
