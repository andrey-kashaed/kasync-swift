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

import Foundation

public protocol Spec {
    associatedtype T
    func isSatisfiedBy(_ obj: T) -> Bool
}

public extension Spec {
    
    func and<S: Spec>(_ spec: S) -> TSpec<T> where S.T == T {
        return AndSpec(self, spec)
    }
    
    func or<S: Spec>(_ spec: S) -> TSpec<T> where S.T == T {
        return OrSpec(self, spec)
    }
    
    func not() -> TSpec<T> {
        return NotSpec(self)
    }
    
}

public class TSpec<T>: Spec {
    
    private let isSatisfiedBy: (T) -> Bool
    
    public init(isSatisfiedBy: @escaping (T) -> Bool) {
        self.isSatisfiedBy = isSatisfiedBy
    }
    
    
    public func isSatisfiedBy(_ obj: T) -> Bool {
        isSatisfiedBy(obj)
    }
    
}

private class AndSpec<T>: TSpec<T> {
    
    public init<S1: Spec, S2: Spec>(_ spec1: S1, _ spec2: S2) where S1.T == T, S2.T == T {
        super.init(isSatisfiedBy: { spec1.isSatisfiedBy($0) && spec2.isSatisfiedBy($0) })
    }
    
}

private class OrSpec<T>: TSpec<T> {
    
    public init<S1: Spec, S2: Spec>(_ spec1: S1, _ spec2: S2) where S1.T == T, S2.T == T {
        super.init(isSatisfiedBy: { spec1.isSatisfiedBy($0) || spec2.isSatisfiedBy($0) })
    }
    
}

private class NotSpec<T>: TSpec<T> {
    
    public init<S: Spec>(_ spec: S) where S.T == T {
        super.init(isSatisfiedBy: { !spec.isSatisfiedBy($0) })
    }
    
}

public class FalseSpec<T>: TSpec<T> {
    
    public init() {
        super.init(isSatisfiedBy: { _ in false })
    }
    
}

public class TrueSpec<T>: TSpec<T> {
    
    public init() {
        super.init(isSatisfiedBy: { _ in true })
    }
    
}
