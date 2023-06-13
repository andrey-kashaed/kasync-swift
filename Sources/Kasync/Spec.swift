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

public protocol Spec<T>: Sendable {
    associatedtype T
    func isSatisfiedBy(_ instance: T) -> Bool
}

public extension Spec {
    
    func and(_ spec: any Spec<T>) -> any Spec<T> {
        AndSpec(self, spec)
    }
    
    func or(_ spec: any Spec<T>) -> any Spec<T> {
        OrSpec(self, spec)
    }
    
    func not() -> any Spec<T> {
        NotSpec(self)
    }
    
}

public func spec<T>(isSatisfiedBy: @Sendable @escaping (T) -> Bool) -> any Spec<T> {
    TSpec(isSatisfiedBy: isSatisfiedBy)
}

public func falseSpec<T>() -> any Spec<T> {
    FalseSpec()
}

public func trueSpec<T>() -> any Spec<T> {
    TrueSpec()
}

fileprivate struct TSpec<T>: Spec {
    
    private let isSatisfiedBy: @Sendable (T) -> Bool
    
    fileprivate init(isSatisfiedBy: @Sendable @escaping (T) -> Bool) {
        self.isSatisfiedBy = isSatisfiedBy
    }
    
    fileprivate func isSatisfiedBy(_ instance: T) -> Bool {
        isSatisfiedBy(instance)
    }
    
}

fileprivate struct AndSpec<T>: Spec {
    
    private let isSatisfiedBy: @Sendable (T) -> Bool
    
    fileprivate init(_ spec1: any Spec<T>, _ spec2: any Spec<T>) {
        isSatisfiedBy = { @Sendable in spec1.isSatisfiedBy($0) && spec2.isSatisfiedBy($0) }
    }
    
    fileprivate func isSatisfiedBy(_ instance: T) -> Bool {
        isSatisfiedBy(instance)
    }
    
}

fileprivate struct OrSpec<T>: Spec {
    
    private let isSatisfiedBy: @Sendable (T) -> Bool
    
    fileprivate init(_ spec1: any Spec<T>, _ spec2: any Spec<T>) {
        isSatisfiedBy = { @Sendable in spec1.isSatisfiedBy($0) || spec2.isSatisfiedBy($0) }
    }
    
    fileprivate func isSatisfiedBy(_ instance: T) -> Bool {
        isSatisfiedBy(instance)
    }
    
}

fileprivate struct NotSpec<T>: Spec {
    
    private let isSatisfiedBy: @Sendable (T) -> Bool
    
    fileprivate init(_ spec: any Spec<T>) {
        isSatisfiedBy = { @Sendable in !spec.isSatisfiedBy($0) }
    }
    
    fileprivate func isSatisfiedBy(_ instance: T) -> Bool {
        isSatisfiedBy(instance)
    }
    
}

fileprivate struct FalseSpec<T>: Spec {
    
    fileprivate init() {}
    
    fileprivate func isSatisfiedBy(_ instance: T) -> Bool {
        false
    }
    
}

fileprivate struct TrueSpec<T>: Spec {
    
    fileprivate init() {}
    
    fileprivate func isSatisfiedBy(_ instance: T) -> Bool {
        true
    }
    
}
