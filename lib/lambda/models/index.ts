export interface Member {
    id: string;
    name: string;
}

export interface Quest {
    id: string;
    name: string;
}

export interface MemberQuest {
    id: string;
    dollarsEarned: number;
    memberId: string;
    questId: string;
}